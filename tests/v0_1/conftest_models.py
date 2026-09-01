from typing import List, Tuple
from pydantic import BaseModel, ConfigDict
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, Session, mapped_column, relationship

from v0_1.infrastructure.adapters.secondary.database.base_sqlalchemy_repo import (
    BaseSQLAlchemyRepository,
)
from v0_1.infrastructure.database.models.base import Base


# ---------------------------------------------------------------------------
# 1. Basic Object (Simple PK)
# ---------------------------------------------------------------------------
class SimpleItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    name: str


class SimpleItemORM(Base):
    __tablename__ = "simple_items"
    __table_args__ = {"schema": "test"}
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class SimpleItemSQLRepo(BaseSQLAlchemyRepository[SimpleItem, SimpleItemORM, str]):
    def __init__(self, session: Session) -> None:
        super().__init__(session=session, domain_cls=SimpleItem, orm_cls=SimpleItemORM)

    def _to_domain(self, orm: SimpleItemORM) -> SimpleItem:
        return SimpleItem.model_validate(orm)

    def _to_orm(self, domain: SimpleItem) -> SimpleItemORM:
        return SimpleItemORM(id=domain.id, name=domain.name)


# ---------------------------------------------------------------------------
# 2. Relationship Object (Nested Collections)
# ---------------------------------------------------------------------------
class OrderItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    product_name: str
    quantity: int


class Order(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    customer: str
    items: List[OrderItem]


class OrderItemORM(Base):
    __tablename__ = "order_items"
    __table_args__ = {"schema": "test"}
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(36), ForeignKey("test.orders.id"))
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    quantity: Mapped[int]


class OrderORM(Base):
    __tablename__ = "orders"
    __table_args__ = {"schema": "test"}
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    customer: Mapped[str] = mapped_column(String(255), nullable=False)
    items: Mapped[List[OrderItemORM]] = relationship(cascade="all, delete-orphan")


class OrderSQLRepo(BaseSQLAlchemyRepository[Order, OrderORM, str]):
    def __init__(self, session: Session) -> None:
        super().__init__(session=session, domain_cls=Order, orm_cls=OrderORM)

    def _to_domain(self, orm: OrderORM) -> Order:
        return Order(
            id=orm.id,
            customer=orm.customer,
            items=[OrderItem.model_validate(i) for i in orm.items],
        )

    def _to_orm(self, domain: Order) -> OrderORM:
        return OrderORM(
            id=domain.id,
            customer=domain.customer,
            items=[
                OrderItemORM(id=i.id, product_name=i.product_name, quantity=i.quantity)
                for i in domain.items
            ],
        )


# ---------------------------------------------------------------------------
# 3 & 4. Composite Primary Key Object (Supports Tuples & Dicts)
# ---------------------------------------------------------------------------
class TenantUser(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    tenant_id: str
    user_id: str
    role: str


class TenantUserORM(Base):
    __tablename__ = "tenant_users"
    __table_args__ = {"schema": "test"}
    tenant_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    role: Mapped[str] = mapped_column(String(50), nullable=False)


class TenantUserSQLRepo(
    BaseSQLAlchemyRepository[TenantUser, TenantUserORM, Tuple[str, str]]
):
    def __init__(self, session: Session) -> None:
        super().__init__(session=session, domain_cls=TenantUser, orm_cls=TenantUserORM)

    def _to_domain(self, orm: TenantUserORM) -> TenantUser:
        return TenantUser.model_validate(orm)

    def _to_orm(self, domain: TenantUser) -> TenantUserORM:
        return TenantUserORM(
            tenant_id=domain.tenant_id, user_id=domain.user_id, role=domain.role
        )
