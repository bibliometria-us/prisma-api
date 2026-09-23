from typing import Any

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


def get_table_args(*args: Any, schema_name: str) -> tuple[Any, ...]:
    """Helper to attach schema configuration cleanly to table_args."""
    return (*args, {"schema": schema_name})
