# tests/test_base.py
from dataclasses import dataclass
from typing import Generator, Optional
import pytest
import redis

from sqlalchemy import String
from sqlalchemy.orm import (
    Mapped,
    MappedColumn,
    Session,
    mapped_column,
)

from v0_1.infrastructure.adapters.secondary.database.base_cached_sql_repo import (
    BaseCachedSQLRepository,
)
from v0_1.infrastructure.adapters.secondary.database.base_redis_repo import (
    BaseRedisRepository,
)
from v0_1.infrastructure.adapters.secondary.database.base_sqlalchemy_repo import (
    BaseSQLAlchemyRepository,
)
from v0_1.infrastructure.database.models.base import Base


# ---------------------------------------------------------------------------
# 1. Test Domain Dataclass
# ---------------------------------------------------------------------------
@dataclass
class DummyItem:
    id: str
    name: str


# ---------------------------------------------------------------------------
# 2. Test SQLAlchemy ORM Model mapped to Global Base
# ---------------------------------------------------------------------------
class DummyItemORM(Base):
    __tablename__ = "dummy_items"
    __table_args__ = {"schema": "test"}

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


# ---------------------------------------------------------------------------
# 3. Dummy Sub-Repositories & Composite Adapter under Test
# ---------------------------------------------------------------------------
class DummyItemSQLRepository(BaseSQLAlchemyRepository[DummyItem, DummyItemORM, str]):
    def __init__(self, session: Session) -> None:
        super().__init__(session=session, domain_cls=DummyItem, orm_cls=DummyItemORM)

    def _to_domain(self, orm_model: DummyItemORM) -> DummyItem:
        return DummyItem(id=orm_model.id, name=orm_model.name)

    def _to_orm(self, domain_entity: DummyItem) -> DummyItemORM:
        return DummyItemORM(id=domain_entity.id, name=domain_entity.name)


class DummyItemCachedRepository(BaseCachedSQLRepository[DummyItem, DummyItemORM, str]):
    def __init__(self, session: Session, redis_client: redis.Redis) -> None:
        sql_repo = DummyItemSQLRepository(session=session)
        redis_repo = BaseRedisRepository[DummyItem, DummyItemORM, str](
            redis_client=redis_client,
            domain_cls=DummyItem,
            orm_cls=DummyItemORM,  # Reads schema='test' & table='dummy_items'
        )
        super().__init__(
            sql_repo=sql_repo,
            redis_repo=redis_repo,
            get_id_func=lambda item: item.id,
        )


# ---------------------------------------------------------------------------
# 4. Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def repo(db_session: Session, redis_client: redis.Redis) -> DummyItemCachedRepository:
    """Instantiates the cached repository using session and redis fixtures."""
    return DummyItemCachedRepository(session=db_session, redis_client=redis_client)


# ---------------------------------------------------------------------------
# 5. Unit Tests
# ---------------------------------------------------------------------------


def test_save_and_get_by_id(
    repo: DummyItemCachedRepository, db_session: Session
) -> None:
    # Arrange
    item = DummyItem(id="item-1", name="Test Item")

    # Act: Saving populates both SQL and Redis cache
    repo.save(item)
    db_session.flush()

    # Assert: Direct read via repo succeeds
    retrieved = repo.get_by_id("item-1")
    assert retrieved is not None
    assert retrieved.id == "item-1"
    assert retrieved.name == "Test Item"

    # Assert: Verify object exists directly in Redis cache
    cached = repo.redis.get("item-1")
    assert cached is not None
    assert cached.name == "Test Item"


def test_get_by_id_hits_cache_and_bypasses_sql_on_repeat_read(
    repo: DummyItemCachedRepository, db_session: Session
) -> None:
    # Arrange: Save directly to SQL, bypassing cache populating save()
    item = DummyItem(id="item-1", name="Database Only Item")
    repo.sql.save(item)
    db_session.flush()

    # Act 1: First fetch misses Redis cache, reads SQL, populates Redis cache
    retrieved_1 = repo.get_by_id("item-1")
    assert retrieved_1 is not None
    assert repo.redis.get("item-1") is not None

    # Act 2: Manually delete from SQL to prove next read comes from Redis
    repo.sql.delete_by_id("item-1")
    db_session.flush()

    # Assert: Second fetch gets entity from Redis cache despite DB deletion
    retrieved_2 = repo.get_by_id("item-1")
    assert retrieved_2 is not None
    assert retrieved_2.name == "Database Only Item"


def test_save_updates_existing_record_and_invalidates_cache(
    repo: DummyItemCachedRepository, db_session: Session
) -> None:
    # Arrange
    item = DummyItem(id="item-1", name="Original Name")
    repo.save(item)
    db_session.flush()

    # Act: Update entity via save
    updated_item = DummyItem(id="item-1", name="Updated Name")
    repo.save(updated_item)
    db_session.flush()

    # Assert
    retrieved = repo.get_by_id("item-1")
    assert retrieved is not None
    assert retrieved.name == "Updated Name"

    cached = repo.redis.get("item-1")
    assert cached is not None
    assert cached.name == "Updated Name"


def test_get_by_id_returns_none_when_not_found(
    repo: DummyItemCachedRepository,
) -> None:
    # Act
    retrieved = repo.get_by_id("non-existent-id")

    # Assert
    assert retrieved is None


def test_list_all(repo: DummyItemCachedRepository, db_session: Session) -> None:
    # Arrange
    item1 = DummyItem(id="item-1", name="Alpha")
    item2 = DummyItem(id="item-2", name="Beta")
    repo.save(item1)
    repo.save(item2)
    db_session.flush()

    # Act
    results = repo.list_all()

    # Assert
    assert len(results) == 2
    ids = {item.id for item in results}
    assert ids == {"item-1", "item-2"}


def test_delete_by_id_removes_from_both_sql_and_redis(
    repo: DummyItemCachedRepository, db_session: Session
) -> None:
    # Arrange
    item = DummyItem(id="item-1", name="To Be Deleted")
    repo.save(item)
    db_session.flush()

    # Verify key is present in Redis before deletion
    assert repo.redis.get("item-1") is not None

    # Act
    repo.delete_by_id("item-1")
    db_session.flush()

    # Assert: Deleted from both SQL and Redis
    assert repo.get_by_id("item-1") is None
    assert repo.redis.get("item-1") is None
    assert repo.sql.get_by_id("item-1") is None
