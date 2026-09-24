# v0_1/infrastructure/adapters/secondary/database/base_sqlalchemy_repo.py
from typing import Any, Generic, TypeVar

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from v0_1.infrastructure.adapters.secondary.database.helpers import (
    extract_primary_key,
)

T = TypeVar("T")  # Domain Entity (e.g., Publicacion)
M = TypeVar("M")  # ORM Model (e.g., PublicacionORM)
K_contra = TypeVar(
    "K_contra", contravariant=True
)  # Primary Key Type (e.g., str or int)


class BaseSQLAlchemyRepository(Generic[T, M, K_contra]):
    """Generic SQLAlchemy repository with automatic primary key extraction."""

    def __init__(self, session: Session, domain_cls: type[T], orm_cls: type[M]) -> None:
        self.session = session
        self.domain_cls = domain_cls
        self.orm_cls = orm_cls

    def _to_domain(self, orm_model: M) -> T:
        raise NotImplementedError

    def _to_orm(self, domain_entity: T) -> M:
        raise NotImplementedError

    def extract_pk(self, identity: Any) -> Any:
        """Extracts primary key formatted for Session.get() using inspection."""
        return extract_primary_key(self.orm_cls, identity)

    def get_by_id(self, entity_id: K_contra) -> T | None:
        pk = self.extract_pk(entity_id)
        orm_model = self.session.get(self.orm_cls, pk)
        return self._to_domain(orm_model) if orm_model else None

    def save(self, entity: T) -> T:
        orm_model = self._to_orm(entity)
        pk = self.extract_pk(orm_model)

        existing = self.session.get(self.orm_cls, pk)
        if existing:
            orm_model = self.session.merge(orm_model)
        else:
            self.session.add(orm_model)

        self.session.flush()
        return self._to_domain(orm_model)

    def delete_by_id(self, entity_id: K_contra) -> None:
        pk = self.extract_pk(entity_id)
        orm_model = self.session.get(self.orm_cls, pk)
        if orm_model:
            self.session.delete(orm_model)

    def _get_total_count(self, stmt: Select[Any]) -> int:
        # Strip ordering/limit/offset for counting performance
        count_stmt = select(func.count()).select_from(
            stmt.order_by(None).limit(None).offset(None).subquery()
        )

        return self.session.scalar(count_stmt) or 0

    def _list_offset(
        self, stmt: Select, page: int | None = None, page_size: int | None = None
    ) -> tuple[list[T], int]:
        """
        Fetches records using direct offset and limit bounds.
        """
        offset = None
        limit = None

        if page and page_size:
            limit = page_size
            offset = (page - 1) * page_size

        # Get total count before slicing
        total_count = self._get_total_count(stmt=stmt)
        # Apply offset if provided
        if offset is not None:
            stmt = stmt.offset(max(0, offset))

        # Apply limit if provided
        if limit is not None:
            stmt = stmt.limit(max(1, limit))

        orm_items = self.session.scalars(stmt).all()
        items = [self._to_domain(item) for item in orm_items]

        return (items, total_count)

    def list_all(self) -> tuple[list[T], int]:
        stmt = select(self.orm_cls)
        return self._list_offset(stmt=stmt)

    def list_all_paginated_offset(
        self, page: int, page_size: int
    ) -> tuple[list[T], int]:
        stmt = select(self.orm_cls)
        return self._list_offset(stmt=stmt, page=page, page_size=page_size)

    def list_filtered_offset(
        self, stmt: Select, page: int | None = None, page_size: int | None = None
    ) -> tuple[list[T], int]:
        return self._list_offset(stmt=stmt, page=page, page_size=page_size)

    def list_all_filtered(self, stmt: Select) -> tuple[list[T], int]:
        return self._list_offset(stmt=stmt)

    def list_filtered_paginated_offset(
        self, stmt: Select, page: int, page_size: int
    ) -> tuple[list[T], int]:
        return self._list_offset(stmt=stmt, page=page, page_size=page_size)
