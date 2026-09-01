# v0_1/infrastructure/adapters/secondary/database/base_redis_repo.py
import json
from typing import Generic, TypeVar

import redis
from pydantic import BaseModel
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Mapper
from sqlalchemy.sql.schema import Table

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
        default_db_name: str | None = "default",
        prefix: str | None = None,
    ) -> None:
        self.redis = redis_client
        self.domain_cls = domain_cls
        self.orm_cls = orm_cls
        self.ttl = ttl_seconds

        self.prefix = prefix or self._build_prefix(default_db_name or "default")

    def _build_prefix(self, default_db_name: str) -> str:
        """Extracts schema/db name and table name from SQLAlchemy ORM metadata."""
        inspected = inspect(self.orm_cls)
        if inspected is None or not isinstance(inspected, Mapper):
            raise TypeError(
                f"Class {self.orm_cls} is not a valid SQLAlchemy ORM model."
            )

        selectable = inspected.persist_selectable
        if not isinstance(selectable, Table):
            raise TypeError(f"Selectable for {self.orm_cls} is not a standard Table.")

        table_name = selectable.name
        schema_name = selectable.schema or default_db_name

        return f"{schema_name}_{table_name}"

    def _format_key(self, entity_id: K_contra) -> str:
        return f"{self.prefix}_{entity_id}"

    def get(self, entity_id: K_contra) -> T | None:
        key = self._format_key(entity_id)
        try:
            raw = self.redis.get(key)
            if isinstance(raw, (bytes, str)):
                data_str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                if isinstance(self.domain_cls, type) and issubclass(
                    self.domain_cls, BaseModel
                ):
                    return self.domain_cls.model_validate_json(data_str)
                data = json.loads(data_str)
                return self.domain_cls(**data)
        except (redis.RedisError, TypeError, ValueError):
            pass
        return None

    def put(self, entity_id: K_contra, entity: T) -> None:
        key = self._format_key(entity_id)
        try:
            if isinstance(entity, BaseModel):
                payload = entity.model_dump_json()
            elif hasattr(entity, "__dict__"):
                payload = json.dumps(entity.__dict__)
            else:
                raise TypeError(f"Entity {entity} cannot be serialized to JSON.")

            self.redis.setex(key, self.ttl, payload)
        except redis.RedisError:
            pass

    def delete(self, entity_id: K_contra) -> None:
        key = self._format_key(entity_id)
        try:
            self.redis.delete(key)
        except redis.RedisError:
            pass
