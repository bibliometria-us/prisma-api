# infrastructure/adapters/secondary/base_redis_repo.py
import json
from dataclasses import asdict, is_dataclass
from typing import Any, Generic, TypeVar, cast

import redis

T = TypeVar("T")
K_contra = TypeVar("K_contra", contravariant=True)


class BaseRedisRepository(Generic[T, K_contra]):
    """Generic Redis cache executor."""

    def __init__(
        self,
        redis_client: redis.Redis,
        domain_cls: type[T],
        prefix: str,
        ttl_seconds: int = 1800,
    ) -> None:
        self.redis = redis_client
        self.domain_cls = domain_cls
        self.prefix = prefix
        self.ttl = ttl_seconds

    def _format_key(self, entity_id: K_contra) -> str:
        return f"{self.prefix}:{entity_id}"

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
