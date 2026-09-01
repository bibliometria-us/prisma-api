# v0_1/infrastructure/adapters/secondary/database/base_sqlalchemy_repo.py
from typing import Any, Generic, TypeVar

from sqlalchemy.orm import Session

from v0_1.infrastructure.adapters.secondary.database.helpers import (
    extract_primary_key,
)

T = TypeVar("T")
M = TypeVar("M")
K_contra = TypeVar("K_contra", contravariant=True)


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

    def list_all(self) -> list[T]:
        orm_models = self.session.query(self.orm_cls).all()
        return [self._to_domain(m) for m in orm_models]
