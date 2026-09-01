# v0_1/infrastructure/adapters/secondary/database/base_redis_repo.py
import json
from typing import Any, Generic, TypeVar, cast

import redis
from pydantic import BaseModel

from v0_1.infrastructure.adapters.secondary.database.helpers import (
    extract_primary_key,
)

T = TypeVar("T")
M = TypeVar("M")
K_contra = TypeVar("K_contra", contravariant=True)


class BaseRedisRepository(Generic[T, M, K_contra]):
    def __init__(
        self,
        redis_client: redis.Redis,
        domain_cls: type[T],
        orm_cls: type[M],
        prefix: str | None = None,
    ) -> None:
        self.redis = redis_client
        self.domain_cls = domain_cls
        self.orm_cls = orm_cls
        self.prefix = prefix or domain_cls.__name__.lower()

    def _format_key(self, entity_id: Any) -> str:
        pk = extract_primary_key(self.orm_cls, entity_id)
        if isinstance(pk, (tuple, list)):
            serialized_pk = ":".join(str(part) for part in pk)
        else:
            serialized_pk = str(pk)
        return f"{self.prefix}:{serialized_pk}"

    def get(self, entity_id: K_contra) -> T | None:
        key = self._format_key(entity_id)
        data = self.redis.get(key)
        if not data:
            return None

        # Cast data to str | bytes for Pydantic's model_validate_json
        raw_json = cast(str | bytes, data)
        cls = cast(type[BaseModel], self.domain_cls)
        return cast(T, cls.model_validate_json(raw_json))

    def put(
        self,
        entity_id: K_contra,
        entity: T,
        pipeline: redis.client.Pipeline | None = None,
    ) -> None:
        key = self._format_key(entity_id)
        if isinstance(entity, BaseModel):
            payload = entity.model_dump_json()
        elif hasattr(entity, "__dict__"):
            payload = json.dumps(entity.__dict__)
        else:
            raise TypeError(f"Entity {entity} cannot be serialized to JSON.")

        target = pipeline if pipeline is not None else self.redis
        target.set(key, payload)

    def delete(
        self,
        entity_id: K_contra,
        pipeline: redis.client.Pipeline | None = None,
    ) -> None:
        key = self._format_key(entity_id)
        target = pipeline if pipeline is not None else self.redis
        target.delete(key)
