# v0_1/application/ports/secondary/repository/unit_of_work_port.py
from types import TracebackType
from typing import Protocol, Self


class UnitOfWorkPort(Protocol):
    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...

    def commit(self) -> None: ...
    def rollback(self) -> None: ...
