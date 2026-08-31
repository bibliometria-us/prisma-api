# infrastructure/adapters/secondary/base_redis_repo.py
import json
from dataclasses import asdict, is_dataclass
from typing import Any, Generic, TypeVar, cast

import redis
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Mapper

T = TypeVar("T")
M = TypeVar("M")
K_contra = TypeVar("K_contra", contravariant=True)


class BaseRedisRepository(Generic[T, M, K_contra]):
    """Generic Redis cache executor with schema/table-scoped key prefixing."""

    def __init__(
        self,
        redis_client: redis.Redis,
        domain_cls: type[T],
        orm_cls: type[M],
        ttl_seconds: int = 1800,
        default_db_name: str = None,
        prefix: str = None,
    ) -> None:
        self.redis = redis_client
        self.domain_cls = domain_cls
        self.orm_cls = orm_cls
        self.ttl = ttl_seconds

        # Build prefix: {db_name}_{table_name}

        self.prefix = prefix or self._build_prefix(default_db_name)

    def _build_prefix(self, default_db_name: str) -> str:
        """Extracts schema/db name and table name from SQLAlchemy ORM metadata."""
        # 1. Obtain mapper from SQLAlchemy inspect
        mapper: Mapper = inspect(self.orm_cls)

        # 2. Type guard to satisfy mypy
        if mapper is None or not hasattr(mapper, "persist_selectable"):
            raise ValueError(
                f"Class {self.orm_cls} is not a valid SQLAlchemy ORM model."
            )

        table_name = mapper.persist_selectable.name
        schema_name = mapper.persist_selectable.schema or default_db_name

        return f"{schema_name}_{table_name}"

    def _format_key(self, entity_id: K_contra) -> str:
        # Generates: {db_name}_{table_name}_{key}
        return f"{self.prefix}_{entity_id}"

    def get(self, entity_id: K_contra) -> T | None:
        key = self._format_key(entity_id)
        try:
            raw = self.redis.get(key)
            if isinstance(raw, (bytes, str)):
                data_str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                data = json.loads(data_str)
                return self.domain_cls(**data)
        except (redis.RedisError, TypeError, ValueError):
            pass
        return None

    def put(self, entity_id: K_contra, entity: T) -> None:
        key = self._format_key(entity_id)
        try:
            if is_dataclass(entity):
                dict_data = asdict(cast(Any, entity))
            elif hasattr(entity, "__dict__"):
                dict_data = entity.__dict__
            else:
                raise ValueError(f"Entity {entity} cannot be serialized to dict.")

            payload = json.dumps(dict_data)
            self.redis.setex(key, self.ttl, payload)
        except redis.RedisError:
            pass

    def delete(self, entity_id: K_contra) -> None:
        key = self._format_key(entity_id)
        try:
            self.redis.delete(key)
        except redis.RedisError:
            pass
