# application/ports/secondary/base_repository_port.py
from typing import Protocol, TypeVar

T = TypeVar("T")  # Domain Entity (e.g., Publicacion)
K_contra = TypeVar("K_contra", contravariant=True)  # Primary Key type (e.g., str, int)


class BaseRepositoryPort(Protocol[T, K_contra]):
    """Generic outbound port defining core persistence contracts."""

    def save(self, entity: T) -> None:
        """Persist or update an entity."""
        ...

    def get_by_id(self, entity_id: K_contra) -> T | None:
        """Retrieve an entity by primary key."""
        ...

    def list_all(self) -> list[T]:
        """Retrieve all entities."""
        ...

    def delete_by_id(self, entity_id: K_contra) -> None:
        """Delete an entity by primary key."""
        ...
