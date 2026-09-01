# v0_1/infrastructure/adapters/secondary/database/base_cached_sql_repo.py
from collections.abc import Callable
from typing import Generic, TypeVar

from v0_1.application.ports.secondary.repository.base_repository_port import (
    BaseRepositoryPort,
)
from v0_1.infrastructure.adapters.secondary.database.base_redis_repo import (
    BaseRedisRepository,
)
from v0_1.infrastructure.adapters.secondary.database.base_sqlalchemy_repo import (
    BaseSQLAlchemyRepository,
)
from v0_1.infrastructure.adapters.secondary.database.unit_of_work import UnitOfWork

T = TypeVar("T")
M = TypeVar("M")
K_contra = TypeVar("K_contra", contravariant=True)


class BaseCachedSQLRepository(BaseRepositoryPort[T, K_contra], Generic[T, M, K_contra]):
    def __init__(
        self,
        sql_repo: BaseSQLAlchemyRepository[T, M, K_contra],
        redis_repo: BaseRedisRepository[T, M, K_contra],
        get_id_func: Callable[[T], K_contra],
        uow: UnitOfWork | None = None,
    ) -> None:
        self.sql = sql_repo
        self.redis = redis_repo
        self._get_id = get_id_func
        self.uow = uow

    def get_by_id(self, entity_id: K_contra) -> T | None:
        cached = self.redis.get(entity_id)
        if cached:
            return cached

        entity = self.sql.get_by_id(entity_id)
        if entity:
            if self.uow:
                self.uow.stage_cache_action(
                    lambda pipe: self.redis.put(entity_id, entity)
                )
            else:
                self.redis.put(entity_id, entity)

        return entity

    def save(self, entity: T) -> None:
        self.sql.save(entity)
        entity_id = self._get_id(entity)

        if self.uow:
            self.uow.stage_cache_action(lambda pipe: self.redis.put(entity_id, entity))
        else:
            self.redis.put(entity_id, entity)

    def delete_by_id(self, entity_id: K_contra) -> None:
        self.sql.delete_by_id(entity_id)

        if self.uow:
            self.uow.stage_cache_action(lambda pipe: self.redis.delete(entity_id))
        else:
            self.redis.delete(entity_id)

    def list_all(self) -> list[T]:
        return self.sql.list_all()
