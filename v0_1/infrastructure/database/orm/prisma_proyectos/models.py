import datetime
import decimal

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    SmallInteger,
    String,
    func,
    text,
)
from sqlalchemy.dialects.mysql import (
    BIGINT,
    DOUBLE,
    INTEGER,
    TINYINT,
)
from sqlalchemy.dialects.mysql import (
    TIMESTAMP as MYSQL_TIMESTAMP,
)
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.orm.base import Base

SCHEMA_NAME = "prisma_proyectos"

# Generic types with MySQL/MariaDB variants
DialectBigInteger = BigInteger().with_variant(
    BIGINT(display_width=20), "mysql", "mariadb"
)
StandardInt = Integer().with_variant(INTEGER(display_width=11), "mysql", "mariadb")
IntTen = Integer().with_variant(INTEGER(display_width=10), "mysql", "mariadb")
TinyIntOne = SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb")
DoublePrecision = Float(precision=53).with_variant(
    DOUBLE(precision=10, scale=2), "mysql", "mariadb"
)

# Timestamp variant keeping MySQL TIMESTAMP behavior
TimestampType = DateTime().with_variant(MYSQL_TIMESTAMP(), "mysql", "mariadb")


class Proyecto(Base):
    __tablename__ = "proyecto"
    __table_args__ = (
        Index("nombre", "nombre", mysql_length={"nombre": 191}),
        Index("referencia", "referencia"),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        DialectBigInteger, primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(String(100), nullable=False)
    nombre: Mapped[str] = mapped_column(String(600), nullable=False)
    inicio: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    ambito: Mapped[str] = mapped_column(String(25), nullable=False)
    competitivo: Mapped[int] = mapped_column(
        TinyIntOne, nullable=False, server_default=text("0")
    )
    visible: Mapped[int] = mapped_column(
        TinyIntOne, nullable=False, server_default=text("1")
    )
    actualizado: Mapped[datetime.datetime] = mapped_column(
        TimestampType,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    referencia: Mapped[str | None] = mapped_column(String(50))
    organica: Mapped[str | None] = mapped_column(String(12))
    fin: Mapped[datetime.date | None] = mapped_column(Date)
    concedido: Mapped[decimal.Decimal | None] = mapped_column(DoublePrecision)
    solicitado: Mapped[decimal.Decimal | None] = mapped_column(DoublePrecision)
    prog_financiador: Mapped[str | None] = mapped_column(String(100))
    entidad_financiadora: Mapped[str | None] = mapped_column(
        String(200), comment="incluidas empresas financiadoras"
    )
    sisius_id: Mapped[int | None] = mapped_column(StandardInt)
    creado: Mapped[datetime.datetime | None] = mapped_column(TimestampType)


class ProyectoIgnorado(Base):
    __tablename__ = "proyecto_ignorado"
    __table_args__ = {"schema": SCHEMA_NAME}

    sisius_id: Mapped[int] = mapped_column(
        StandardInt, primary_key=True, server_default=text("0")
    )


class ProyectoMiembro(Base):
    __tablename__ = "proyecto_miembro"
    __table_args__ = (
        Index("investigador_id", "investigador_id"),
        Index("proyecto_id", "proyecto_id"),
        Index(
            "proyecto_miembro_proyecto_id_IDX",
            "proyecto_id",
            "firma",
            unique=True,
        ),
        Index(
            "proyecto_miembro_proyecto_id_firma_rol",
            "proyecto_id",
            "firma",
            "rol",
            unique=True,
        ),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        DialectBigInteger, primary_key=True, autoincrement=True
    )
    proyecto_id: Mapped[int] = mapped_column(DialectBigInteger, nullable=False)
    firma: Mapped[str] = mapped_column(String(250), nullable=False)
    rol: Mapped[str] = mapped_column(String(50), nullable=False)
    investigador_id: Mapped[int | None] = mapped_column(IntTen)
    fecha_alta: Mapped[datetime.date | None] = mapped_column(Date)
    fecha_baja: Mapped[datetime.date | None] = mapped_column(Date)
