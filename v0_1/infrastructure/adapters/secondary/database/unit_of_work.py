import inspect
import logging
from collections.abc import Callable
from types import TracebackType

import redis
import redis.client
from sqlalchemy.orm import Session
from typing_extensions import Self

logger = logging.getLogger(__name__)


class UnitOfWork:
    def __init__(
        self, session: Session, redis_client: redis.Redis | None = None
    ) -> None:
        self.session = session
        self.redis = redis_client
        self._staged_cache_actions: list[Callable] = []
        self.pipeline: redis.client.Pipeline | None = (
            self.redis.pipeline() if self.redis else None
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type:
            self.rollback()
        else:
            self.commit()

    def stage_cache_action(self, action: Callable) -> None:
        self._staged_cache_actions.append(action)

    def commit(self) -> None:
        try:
            # 1. First-class persistence commit in SQL
            self.session.commit()

            # 2. Sequential post-commit cache operations
            if self.redis and self._staged_cache_actions:
                for action in self._staged_cache_actions:
                    try:
                        sig = inspect.signature(action)
                        if len(sig.parameters) > 0:
                            # Pass standard redis client instead of pipeline
                            action(self.redis)
                        else:
                            action()
                    except (redis.RedisError, AttributeError, TypeError) as cache_exc:
                        logger.error(
                            "Failed to execute staged cache action post-commit: %s",
                            cache_exc,
                        )

            self._staged_cache_actions.clear()
        except Exception:
            self.rollback()
            raise

    def rollback(self) -> None:
        self.session.rollback()
        self._staged_cache_actions.clear()
