# app/repositories/base.py
from typing import Generic, TypeVar, Type, Optional, Sequence, Any, Union, Dict, Tuple
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from v0_1.database import Base

# Define a generic type bound to SQLAlchemy's DeclarativeBase
ModelType = TypeVar("ModelType", bound=Base)
PKType = Union[Any, Tuple[Any, ...], Dict[str, Any]]


class BaseRepository(Generic[ModelType]):
    """
    Abstract Base Repository providing common CRUD and query operations.

    :param model: The SQLAlchemy model class
    :param session: The active SQLAlchemy session
    """

    @property
    def dialect_name(self) -> str:
        return self.session.bind.dialect.name if self.session.bind else ""

    def __init__(self, model: Type[ModelType], session: Session):
        self.model = model
        self.session = session

    def get_by_id(self, id: PKType) -> Optional[ModelType]:
        return self.session.get(self.model, id)

    def get_all(self, limit: int = 100, offset: int = 0) -> Sequence[ModelType]:
        stmt = select(self.model).limit(limit).offset(offset)
        return self.session.scalars(stmt).all()

    def filter_by(self, **kwargs: Any) -> Sequence[ModelType]:
        stmt = select(self.model).filter_by(**kwargs)
        return self.session.scalars(stmt).all()

    def count(self, **kwargs: Any) -> int:
        stmt = select(func.count()).select_from(self.model)
        if kwargs:
            stmt = stmt.filter_by(**kwargs)
        return self.session.scalar(stmt) or 0

    def create(self, **attributes: Any) -> ModelType:
        instance = self.model(**attributes)
        self.session.add(instance)
        self.session.flush()
        return instance

    def add(self, instance: ModelType) -> ModelType:
        self.session.add(instance)
        self.session.flush()
        return instance

    def delete(self, instance: ModelType) -> None:
        self.session.delete(instance)
        self.session.flush()

    def delete_by_id(self, id: PKType) -> bool:
        instance = self.get_by_id(id)
        if instance:
            self.delete(instance)
            return True
        return False
