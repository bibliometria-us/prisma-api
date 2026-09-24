# v0_1/infrastructure/adapters/secondary/database/base_cached_sql_repo.py
from collections.abc import Callable
from functools import partial
from typing import Any, Generic, ParamSpec, TypeVar, cast

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

T = TypeVar("T", bound=BaseModel)  # Domain Entity (e.g., Publicacion)
M = TypeVar("M")  # ORM Model (e.g., PublicacionORM)
K_contra = TypeVar(
    "K_contra", contravariant=True
)  # Primary Key Type (e.g., str, int, list or dict)
P = ParamSpec("P")


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

    def get_by_id(self, entity_id: Any) -> T | None:
        cached = self.redis.get(entity_id)
        if cached is not None:
            return cached

        entity = self.sql.get_by_id(entity_id)
        if entity is None:
            return None

        uow = cast(UnitOfWork, getattr(self, "uow", None))
        if uow is not None:
            # Defer cache population until post-commit
            uow.stage_cache_action(
                lambda pipe=None, eid=entity_id, e=entity: self.redis.put(
                    eid, e, pipeline=pipe
                )
            )
        else:
            self.redis.put(entity_id, entity)

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
            self.uow.stage_cache_action(
                lambda pipe=None: self.redis.clear_related_cache(pipeline=pipe)
            )
        else:
            self.redis.put(entity_id, saved_entity)
            self.redis.clear_related_cache()

        return saved_entity

    def delete(self, entity: T) -> None:
        entity_id = self._extract_id_from_entity(entity=entity)
        self.delete_by_id(entity_id=entity_id)

    def delete_by_id(self, entity_id: K_contra) -> None:
        self.sql.delete_by_id(entity_id)

        if self.uow:
            self.uow.stage_cache_action(
                lambda pipe=None: self.redis.delete(entity_id, pipeline=pipe)
            )
            self.uow.stage_cache_action(
                lambda pipe=None: self.redis.clear_related_cache(pipeline=pipe)
            )
        else:
            self.redis.delete(entity_id)
            self.redis.clear_related_cache()

    def _fetch_cached_list(
        self,
        cache_fetch_fn: Callable[[], tuple[list[T], int]],
        sql_fetch_fn: Callable[[], tuple[list[T], int]],
        cache_type: str,
        entity_id: Any = None,
    ) -> tuple[list[T], int]:
        """Generic cache-aside wrapper for multi-entity reads.

        Tries cache first; on miss, queries SQL database and stages
        the population of Redis via Unit of Work or direct write.
        """
        # 1. Try Cache Read
        cached, total = cache_fetch_fn()
        if cached:
            return cached, total

        # 2. Database Fallback on Cache Miss
        total = 0
        entities, total = sql_fetch_fn()

        # 3. Cache Population on Hit
        if entities:
            # Helper lambda to write payload into Redis
            def write_cache(pipe: Any = None) -> None:
                self.redis.put(
                    cast(Any, entity_id),
                    entities,
                    pipeline=pipe,
                    type=cache_type,
                )
                self.redis.put_int(
                    key=self.redis._total_prefix(),
                    value=total,
                    pipeline=pipe,
                )

            uow = cast(UnitOfWork, getattr(self, "uow", None))
            if uow is not None:
                uow.stage_cache_action(lambda pipe=None: write_cache(pipe=pipe))
            else:
                write_cache()

        return entities, total

    def list_all(self) -> tuple[list[T], int]:
        return self._fetch_cached_list(
            cache_fetch_fn=self.redis.list_all,
            sql_fetch_fn=self.sql.list_all,
            cache_type="all",
        )

    def list_paginated_offset(self, page: int, page_size: int) -> tuple[list[T], int]:
        return self._fetch_cached_list(
            cache_fetch_fn=lambda: self.redis.list_page(page, page_size),
            sql_fetch_fn=lambda: self.sql.list_all_paginated_offset(page, page_size),
            cache_type="page",
        )

    def list_all_filtered(
        self,
        sql_fetch_fn: Callable[P, tuple[list[T], int]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> tuple[list[T], int]:
        p_cache_fetch_fn = partial(self.redis.list_all, **kwargs)
        p_sql_fetch_fn = partial(sql_fetch_fn, **kwargs)

        return self._fetch_cached_list(
            cache_fetch_fn=p_cache_fetch_fn,
            sql_fetch_fn=p_sql_fetch_fn,
            cache_type="all",
        )

    def list_paginated_filtered_offset(
        self,
        sql_fetch_fn: Callable[..., tuple[list[T], int]],
        page: int,
        page_size: int,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[list[T], int]:
        # Combine page arguments with remaining kwargs
        combined_kwargs = {"page": page, "page_size": page_size, **kwargs}

        p_cache_fetch_fn = partial(self.redis.list_page, **combined_kwargs)
        p_sql_fetch_fn = partial(sql_fetch_fn, **combined_kwargs)

        return self._fetch_cached_list(
            cache_fetch_fn=p_cache_fetch_fn,
            sql_fetch_fn=p_sql_fetch_fn,
            cache_type="page",
        )
