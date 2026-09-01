# v0_1/infrastructure/adapters/secondary/database/unit_of_work.py
from collections.abc import Callable
from types import TracebackType
from typing import Self

import redis
from sqlalchemy.orm import Session

from v0_1.application.ports.secondary.repository.unit_of_work_port import UnitOfWorkPort


class UnitOfWork(UnitOfWorkPort):
    def __init__(
        self, session: Session, redis_client: redis.Redis | None = None
    ) -> None:
        self.session = session
        self.redis = redis_client
        self._staged_cache_actions: list[Callable[[redis.client.Pipeline], None]] = []

    def stage_cache_action(
        self, action: Callable[[redis.client.Pipeline], None]
    ) -> None:
        self._staged_cache_actions.append(action)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()

    def commit(self) -> None:
        try:
            self.session.commit()
            if self.redis and self._staged_cache_actions:
                try:
                    pipe = self.redis.pipeline()
                    for action in self._staged_cache_actions:
                        action(pipe)
                    pipe.execute()
                except redis.RedisError:
                    pass
        except Exception:
            self.rollback()
            raise
        finally:
            self._staged_cache_actions.clear()

    def rollback(self) -> None:
        self.session.rollback()
        self._staged_cache_actions.clear()
