# v0_1/infrastructure/adapters/secondary/database/base_redis_repo.py
import json
from typing import Any, Generic, ParamSpec, TypeVar, cast

import redis
import redis.client
from pydantic import BaseModel

from v0_1.infrastructure.adapters.secondary.database.helpers import (
    extract_primary_key,
)

T = TypeVar("T", bound=BaseModel)
M = TypeVar("M")
K_contra = TypeVar("K_contra", contravariant=True)
P = ParamSpec("P")


class BaseRedisRepository(Generic[T, M, K_contra]):
    def __init__(
        self,
        redis_client: redis.Redis,
        domain_cls: type[T],
        orm_cls: type[M],
        prefix: str | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> None:
        self.redis = redis_client
        self.domain_cls = domain_cls
        self.orm_cls = orm_cls
        self.prefix = (prefix or domain_cls.__name__).lower()
        self.page = page
        self.page_size = page_size
        self.query_kwargs: dict[str, Any] = {}

    def _get_key(
        self, entity_id: K_contra | None = None, type: str = "entity", **kwargs: Any
    ) -> str:
        if kwargs:
            self.query_kwargs = kwargs
        if type == "entity" and entity_id:
            return self._id_prefix(entity_id=entity_id)

        if type == "all":
            return self._all_key()

        if type == "page" and self.page_size:
            return self._page_prefix()

        if type == "total":
            return self._total_prefix()

        raise ValueError("Unvalid set of inputs to calculate redis key.")

    def _prefix(self) -> str:
        kwargs_prefix = ":".join(f"{k!s}:{v!s}" for k, v in self.query_kwargs.items())
        return ":".join(part for part in (self.prefix, kwargs_prefix) if part)

    def _id_prefix(self, entity_id: K_contra) -> str:
        pk = extract_primary_key(self.orm_cls, entity_id)
        if isinstance(pk, (tuple, list)):
            serialized_pk = ":".join(str(part) for part in pk)
        else:
            serialized_pk = str(pk)

        return ":".join(
            item for item in (self._prefix(), serialized_pk) if item is not None
        )

    def _page_prefix(self) -> str:
        page = self.page or 1
        page_prefix = f"page:{page}:size:{self.page_size}:{self._prefix()}"

        return page_prefix

    def _total_prefix(self) -> str:
        return f"total:{self._prefix()}"

    def _all_key(self) -> str:
        return f"all:{self._prefix()}"

    def _set_page(self, page: int, page_size: int) -> None:
        self.page = page
        self.page_size = page_size

    def _get_int(self, key: str) -> int | None:
        data = cast(bytes, self.redis.get(key))

        if not data or not data.isdigit():
            return None

        return int(data)

    def _get(self, key: str) -> T | None:
        data = self.redis.get(key)

        if not data:
            return None

        # Cast data to str | bytes for Pydantic's model_validate_json
        raw_json = cast(str | bytes, data)
        cls = cast(type[BaseModel], self.domain_cls)

        return cast(T, cls.model_validate_json(raw_json))

    def get(self, entity_id: K_contra) -> T | None:
        key = self._id_prefix(entity_id)
        return self._get(key)

    def put_int(
        self,
        value: int,
        key: str,
        pipeline: redis.client.Pipeline | None = None,
    ) -> None:
        target = pipeline if pipeline is not None else self.redis
        target.set(key, value)

    def put(
        self,
        entity_id: K_contra,
        entity: T | list[T],
        pipeline: redis.client.Pipeline | None = None,
        type: str = "entity",
    ) -> None:
        key = self._get_key(entity_id, type=type)

        # 1. Pydantic BaseModel instance
        if isinstance(entity, BaseModel):
            payload = entity.model_dump_json()

        # 2. List or Primitive Types (e.g., list[BaseModel] or list[dict])
        elif isinstance(entity, list):
            payload = json.dumps(
                [
                    (
                        item.model_dump(mode="json")
                        if isinstance(item, BaseModel)
                        else item
                    )
                    for item in entity
                ]
            )

        # 3. Standard objects with __dict__
        elif hasattr(entity, "__dict__"):
            payload = json.dumps(entity.__dict__, default=str)

        # 4. Direct JSON serializable types (dicts, str, int)
        else:
            try:
                payload = json.dumps(entity, default=str)
            except (TypeError, ValueError) as exc:
                raise TypeError(
                    f"Entity of type {type(entity).__name__} cannot be serialized to JSON."
                ) from exc

        target = pipeline if pipeline is not None else self.redis
        target.set(key, payload)

    def delete(
        self,
        entity_id: K_contra,
        pipeline: redis.client.Pipeline | None = None,
    ) -> None:
        key = self._get_key(entity_id)
        target = pipeline if pipeline is not None else self.redis
        target.delete(key)

    def delete_by_pattern(
        self,
        pattern: str,
        pipeline: redis.client.Pipeline | None = None,
        batch_size: int = 1000,
    ) -> int:
        cursor: Any = "0"
        total_deleted = 0

        target = pipeline if pipeline is not None else self.redis

        while True:
            # Cast the scan return value to explicitly inform MyPy of sync execution
            scan_result = cast(
                tuple[Any, list[Any]],
                self.redis.scan(cursor=cursor, match=pattern, count=batch_size),
            )
            cursor, keys = scan_result

            if keys:
                # Cast keys to a list of str/bytes for safe variadic unpacking
                key_list = [
                    k.decode("utf-8") if isinstance(k, bytes) else str(k) for k in keys
                ]
                target.delete(*key_list)
                total_deleted += len(keys)

            if cursor in (0, "0", b"0"):
                break

        return total_deleted

    def clear_related_cache(
        self,
        pipeline: redis.client.Pipeline | None = None,
    ) -> int:
        pattern = f"*:{self._prefix()}"
        return self.delete_by_pattern(pattern=pattern, pipeline=pipeline)

    def _get_list(self, key: str) -> tuple[list[T], int]:
        """Retrieves all domain entities from a single JSON array key in Redis."""
        try:
            raw_entries = cast(bytes | str | None, self.redis.get(key))

            if not raw_entries:
                return [], 0

            # 1. Decode bytes if returned by the Redis driver
            json_str: str = (
                raw_entries.decode("utf-8")
                if isinstance(raw_entries, bytes)
                else raw_entries
            )

            # 2. Parse JSON list payload
            data = json.loads(json_str)

            if not isinstance(data, list):
                return [], 0

            # 3. Deserialize items into domain entities
            items: list[T] = [self.domain_cls.model_validate(item) for item in data]

            # Save total amount
            total_key = self._get_key(type="total")
            total = self._get_int(total_key) or 0

            return items, total

        except redis.RedisError:
            return [], 0
        except json.JSONDecodeError:
            return [], 0

    def _list(self, type: str, **kwargs: Any) -> tuple[list[T], int]:
        key = self._get_key(type=type, **kwargs)
        return self._get_list(key)

    def list_all(self, **kwargs: Any) -> tuple[list[T], int]:
        return self._list(type="all", **kwargs)

    def list_page(
        self, page: int, page_size: int, **kwargs: Any
    ) -> tuple[list[T], int]:
        self._set_page(page, page_size)
        return self._list(type="page", **kwargs)
