# infrastructure/adapters/secondary/base_sqlalchemy_repo.py
from typing import Generic, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session

T = TypeVar("T")
M = TypeVar("M")
K_contra = TypeVar("K_contra", contravariant=True)


class BaseSQLAlchemyRepository(Generic[T, M, K_contra]):
    """Generic SQL executor."""

    def __init__(self, session: Session, domain_cls: type[T], orm_cls: type[M]) -> None:
        self.session = session
        self.domain_cls = domain_cls
        self.orm_cls = orm_cls

    def _to_domain(self, orm_model: M) -> T:
        raise NotImplementedError

    def _to_orm(self, domain_entity: T) -> M:
        raise NotImplementedError

    def get_by_id(self, entity_id: K_contra) -> T | None:
        orm_entity = self.session.get(self.orm_cls, entity_id)
        return self._to_domain(orm_entity) if orm_entity else None

    def save(self, entity: T) -> None:
        orm_entity = self._to_orm(entity)
        self.session.merge(orm_entity)

    def delete_by_id(self, entity_id: K_contra) -> None:
        orm_entity = self.session.get(self.orm_cls, entity_id)
        if orm_entity:
            self.session.delete(orm_entity)

    def list_all(self) -> list[T]:
        stmt = select(self.orm_cls)
        records = self.session.scalars(stmt).all()
        return [self._to_domain(rec) for rec in records]
