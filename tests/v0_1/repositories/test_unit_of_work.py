# tests/test_unit_of_work.py
import pytest
import redis
from pydantic import BaseModel, ConfigDict
from sqlalchemy import String
from sqlalchemy.orm import Mapped, Session, mapped_column

from v0_1.infrastructure.adapters.secondary.database.base_cached_sql_repo import (
    BaseCachedSQLRepository,
)
from v0_1.infrastructure.adapters.secondary.database.base_redis_repo import (
    BaseRedisRepository,
)
from v0_1.infrastructure.adapters.secondary.database.base_sqlalchemy_repo import (
    BaseSQLAlchemyRepository,
)
from v0_1.infrastructure.adapters.secondary.database.unit_of_work import UnitOfWork
from v0_1.infrastructure.database.models.base import Base


# ---------------------------------------------------------------------------
# 1. Setup Domain Model & ORM
# ---------------------------------------------------------------------------
class DummyEntity(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str


class DummyEntityORM(Base):
    __tablename__ = "uow_dummy_items"
    __table_args__ = {"schema": "test"}

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class DummyEntitySQLRepository(
    BaseSQLAlchemyRepository[DummyEntity, DummyEntityORM, str]
):
    def __init__(self, session: Session) -> None:
        super().__init__(
            session=session, domain_cls=DummyEntity, orm_cls=DummyEntityORM
        )

    def _to_domain(self, orm_model: DummyEntityORM) -> DummyEntity:
        return DummyEntity.model_validate(orm_model)

    def _to_orm(self, domain_entity: DummyEntity) -> DummyEntityORM:
        return DummyEntityORM(id=domain_entity.id, name=domain_entity.name)


class DummyEntityCachedRepository(
    BaseCachedSQLRepository[DummyEntity, DummyEntityORM, str]
):
    def __init__(
        self, session: Session, redis_client: redis.Redis, uow: UnitOfWork
    ) -> None:
        sql_repo = DummyEntitySQLRepository(session=session)
        redis_repo = BaseRedisRepository[DummyEntity, DummyEntityORM, str](
            redis_client=redis_client,
            domain_cls=DummyEntity,
            orm_cls=DummyEntityORM,
        )
        super().__init__(
            sql_repo=sql_repo,
            redis_repo=redis_repo,
            get_id_func=lambda item: item.id,
            uow=uow,
        )


# ---------------------------------------------------------------------------
# 2. Unit Tests
# ---------------------------------------------------------------------------
def test_uow_commits_database_and_executes_staged_cache_actions(
    db_session: Session, redis_client: redis.Redis
) -> None:
    uow = UnitOfWork(session=db_session, redis_client=redis_client)
    repo = DummyEntityCachedRepository(
        session=db_session, redis_client=redis_client, uow=uow
    )
    entity = DummyEntity(id="uow-1", name="Staged Entity")

    with uow:
        repo.save(entity)
        assert repo.redis.get("uow-1") is None

    retrieved = repo.get_by_id("uow-1")
    assert retrieved is not None
    assert retrieved.name == "Staged Entity"
    assert repo.redis.get("uow-1") is not None


def test_uow_rollbacks_database_and_discards_staged_cache_actions_on_exception(
    db_session: Session, redis_client: redis.Redis
) -> None:
    uow = UnitOfWork(session=db_session, redis_client=redis_client)
    repo = DummyEntityCachedRepository(
        session=db_session, redis_client=redis_client, uow=uow
    )
    entity = DummyEntity(id="uow-2", name="Should Be Rolled Back")

    with pytest.raises(RuntimeError, match="Force Rollback"):
        with uow:
            repo.save(entity)
            raise RuntimeError("Force Rollback")

    assert repo.sql.get_by_id("uow-2") is None
    assert repo.redis.get("uow-2") is None


def test_uow_staged_deletion_invalidates_cache_only_on_commit(
    db_session: Session, redis_client: redis.Redis
) -> None:
    uow = UnitOfWork(session=db_session, redis_client=redis_client)
    repo = DummyEntityCachedRepository(
        session=db_session, redis_client=redis_client, uow=uow
    )
    entity = DummyEntity(id="uow-3", name="Initial")

    repo.sql.save(entity)
    repo.redis.put("uow-3", entity)
    db_session.commit()

    with uow:
        repo.delete_by_id("uow-3")
        assert repo.redis.get("uow-3") is not None

    assert repo.redis.get("uow-3") is None
    assert repo.sql.get_by_id("uow-3") is None
