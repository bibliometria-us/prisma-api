from typing import Any

from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Mapper


def extract_primary_key(orm_cls: type[Any], identity: Any) -> Any:
    """Extracts a scalar or tuple primary key suitable for Session.get().

    Supports:
    - Scalar values (single PK)
    - Tuples / Lists (composite PK)
    - Dictionaries mapping column names to values
    - ORM model instances
    """
    mapper: Mapper = inspect(orm_cls)
    pk_columns = [col.name for col in mapper.primary_key]

    # Case 1: Identity is already an ORM model instance
    if isinstance(identity, orm_cls):
        pk_values = tuple(getattr(identity, col_name) for col_name in pk_columns)
        return pk_values[0] if len(pk_values) == 1 else pk_values

    # Case 2: Identity is a Dictionary
    if isinstance(identity, dict):
        missing_keys = set(pk_columns) - set(identity.keys())
        if missing_keys:
            raise KeyError(
                f"Missing primary key columns {missing_keys} for model {orm_cls.__name__}. "
                f"Expected keys: {pk_columns}"
            )
        pk_values = tuple(identity[col_name] for col_name in pk_columns)
        return pk_values[0] if len(pk_values) == 1 else pk_values

    # Case 3: Identity is a Tuple or List
    if isinstance(identity, (tuple, list)):
        if len(identity) != len(pk_columns):
            raise ValueError(
                f"Expected composite primary key of length {len(pk_columns)} "
                f"for model {orm_cls.__name__}, but got {len(identity)} elements."
            )
        return tuple(identity)

    # Case 4: Single Scalar Identity
    if len(pk_columns) > 1:
        raise ValueError(
            f"Model {orm_cls.__name__} has a composite primary key {pk_columns}, "
            f"but a single scalar identity was provided: {identity}"
        )

    return identity
