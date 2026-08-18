from typing import Optional
import datetime
import decimal

from sqlalchemy import (
    Column,
    DECIMAL,
    Date,
    DateTime,
    Float,
    Index,
    String,
    TIMESTAMP,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.mysql import (
    BIGINT,
    INTEGER,
    MEDIUMTEXT,
    SMALLINT,
    TINYINT,
    VARCHAR,
)
from sqlalchemy.orm import Mapped, mapped_column
from v0_1.database import Base


class AConfiguracion(Base):
    __tablename__ = "a_configuracion"
    __table_args__ = {"schema": "prisma"}

    variable: Mapped[str] = mapped_column(String(25), primary_key=True)
    valor: Mapped[str] = mapped_column(String(150), nullable=False)
    editable: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("1")
    )
