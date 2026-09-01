# v0_1/infrastructure/adapters/secondary/database/base_cached_sql_repo.py
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from v0_1.infrastructure.adapters.secondary.database.base_redis_repo import (
    BaseRedisRepository,
)
from v0_1.infrastructure.adapters.secondary.database.base_sqlalchemy_repo import (
    BaseSQLAlchemyRepository,
)
from v0_1.infrastructure.adapters.secondary.database.helpers import (
    extract_primary_key,
)
from v0_1.infrastructure.adapters.secondary.database.unit_of_work import (
    UnitOfWork,
)

T = TypeVar("T")
M = TypeVar("M")
K_contra = TypeVar("K_contra", contravariant=True)


class BaseCachedSQLRepository(Generic[T, M, K_contra]):
    def __init__(
        self,
        sql_repo: BaseSQLAlchemyRepository[T, M, K_contra],
        redis_repo: BaseRedisRepository[T, M, K_contra],
        uow: UnitOfWork | None = None,
    ) -> None:
        self.sql = sql_repo
        self.redis = redis_repo
        self.uow = uow

    def _extract_id_from_entity(self, entity: T | None) -> Any:
        if entity is None:
            return None

        if isinstance(entity, BaseModel) or not isinstance(entity, self.sql.orm_cls):
            orm_instance = self.sql._to_orm(entity)
            return extract_primary_key(self.sql.orm_cls, orm_instance)

        return extract_primary_key(self.sql.orm_cls, entity)

    def get_by_id(self, entity_id: K_contra) -> T | None:
        cached = self.redis.get(entity_id)
        if cached is not None:
            return cached

        entity = self.sql.get_by_id(entity_id)
        if entity is not None:
            db_id = self._extract_id_from_entity(entity)
            if self.uow:
                self.uow.stage_cache_action(
                    lambda pipe=None: self.redis.put(db_id, entity, pipeline=pipe)
                )
            else:
                self.redis.put(db_id, entity)

        return entity

    def save(self, entity: T) -> T:
        saved_entity = self.sql.save(entity)
        if saved_entity is None:
            return entity

        entity_id = self._extract_id_from_entity(saved_entity)

        if self.uow:
            self.uow.stage_cache_action(
                lambda pipe=None: self.redis.put(entity_id, saved_entity, pipeline=pipe)
            )
        else:
            self.redis.put(entity_id, saved_entity)

        return saved_entity

    def delete_by_id(self, entity_id: K_contra) -> None:
        self.sql.delete_by_id(entity_id)

        if self.uow:
            self.uow.stage_cache_action(
                lambda pipe=None: self.redis.delete(entity_id, pipeline=pipe)
            )
        else:
            self.redis.delete(entity_id)

    def list_all(self) -> list[T]:
        return self.sql.list_all()
