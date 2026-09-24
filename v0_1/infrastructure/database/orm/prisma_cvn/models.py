import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.mysql import BIGINT, INTEGER, TEXT, TIMESTAMP, VARCHAR
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.orm.base import Base

SCHEMA_NAME = "prisma_cvn"

# Generic type definitions with corrected MySQL/MariaDB variants:
UnsignedBigInteger = BigInteger().with_variant(
    BIGINT(display_width=20, unsigned=True), "mysql", "mariadb"
)
DialectBigInteger = BigInteger().with_variant(
    BIGINT(display_width=20), "mysql", "mariadb"
)
SmallIntThree = Integer().with_variant(INTEGER(display_width=3), "mysql", "mariadb")
StandardInt = Integer().with_variant(INTEGER(display_width=11), "mysql", "mariadb")

# Timestamp type variant to preserve MySQL TIMESTAMP DDL signature
TimestampType = DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb")

Varchar100Spanish = String(100).with_variant(
    VARCHAR(100, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
    "mysql",
    "mariadb",
)
Varchar350Spanish = String(350).with_variant(
    VARCHAR(350, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
    "mysql",
    "mariadb",
)
Varchar150Spanish4 = String(150).with_variant(
    VARCHAR(150, charset="utf8mb4", collation="utf8mb4_spanish_ci"),
    "mysql",
    "mariadb",
)
TextSpanish = Text().with_variant(
    TEXT(charset="utf8mb3", collation="utf8mb3_spanish_ci"), "mysql", "mariadb"
)


t_cvn_categoria_norm = Table(
    "cvn_categoria_norm",
    Base.metadata,
    Column("id_categoria", String(6), nullable=False),
    Column("nombre", String(150), nullable=False),
    schema=SCHEMA_NAME,
)


class Paises(Base):
    __tablename__ = "paises"
    __table_args__ = {"schema": SCHEMA_NAME}

    identificador: Mapped[int] = mapped_column(SmallIntThree, primary_key=True)
    pais: Mapped[str | None] = mapped_column(String(38))


class Peticion(Base):
    __tablename__ = "peticion"
    __table_args__ = (
        Index("idx_peticion_entrega", "entrega"),
        Index("idx_peticion_reintentos", "reintentos"),
        Index("idx_peticion_solicitante", "solicitante"),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        UnsignedBigInteger, primary_key=True, autoincrement=True
    )
    solicitante: Mapped[str] = mapped_column(
        Varchar100Spanish,
        nullable=False,
        comment="UVUS del solicitante",
    )
    solicitud: Mapped[datetime.datetime] = mapped_column(
        TimestampType,
        nullable=False,
        server_default=func.now(),
        comment="Fecha de solicitud del CV",
    )
    tipo: Mapped[str] = mapped_column(String(15), nullable=False)
    investigador_id: Mapped[int] = mapped_column(DialectBigInteger, nullable=False)
    cvn: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Datos del CVN a generar"
    )
    reintentos: Mapped[int] = mapped_column(
        SmallIntThree, nullable=False, server_default=text("5")
    )
    entrega: Mapped[datetime.datetime | None] = mapped_column(
        TimestampType,
        comment="Fecha de entrega del CV o de la información del error",
    )
    respuesta: Mapped[str | None] = mapped_column(
        TextSpanish,
        comment="Respuesta dada por la FECYT",
    )


class PeticionPreproduccion(Base):
    __tablename__ = "peticion_preproduccion"
    __table_args__ = (
        Index("idx_peticion_preproduccion_solicitante", "solicitante"),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        UnsignedBigInteger, primary_key=True, autoincrement=True
    )
    solicitante: Mapped[str] = mapped_column(
        Varchar100Spanish,
        nullable=False,
        comment="UVUS del solicitante",
    )
    solicitud: Mapped[datetime.datetime] = mapped_column(
        TimestampType,
        nullable=False,
        server_default=func.now(),
        comment="Fecha de solicitud del CV",
    )
    tipo: Mapped[str] = mapped_column(String(15), nullable=False)
    investigador_id: Mapped[int] = mapped_column(DialectBigInteger, nullable=False)
    cvn: Mapped[str] = mapped_column(
        TextSpanish,
        nullable=False,
        comment="Datos del CVN a generar",
    )
    entrega: Mapped[datetime.datetime | None] = mapped_column(
        TimestampType,
        comment="Fecha de entrega del CV o de la información del error",
    )
    respuesta: Mapped[str | None] = mapped_column(
        TextSpanish,
        comment="Respuesta dada por la FECYT",
    )


class ProgDoctorado(Base):
    __tablename__ = "prog_doctorado"
    __table_args__ = (
        Index("idx_prog_doctorado_nombre", "nombre", mysql_length={"nombre": 255}),
        {"schema": SCHEMA_NAME},
    )

    codigo: Mapped[int] = mapped_column(StandardInt, primary_key=True)
    nombre: Mapped[str] = mapped_column(Varchar350Spanish, nullable=False)
    delegado: Mapped[int | None] = mapped_column(StandardInt)


class Unesco(Base):
    __tablename__ = "unesco"
    __table_args__ = (
        Index("idx_unesco_eng", "eng"),
        Index("idx_unesco_spa", "spa"),
        {"schema": SCHEMA_NAME},
    )

    cod1: Mapped[str] = mapped_column(String(2), primary_key=True)
    cod2: Mapped[str] = mapped_column(String(2), primary_key=True)
    cod3: Mapped[str] = mapped_column(String(2), primary_key=True)
    spa: Mapped[str] = mapped_column(Varchar150Spanish4, nullable=False)
    eng: Mapped[str] = mapped_column(String(150), nullable=False)
