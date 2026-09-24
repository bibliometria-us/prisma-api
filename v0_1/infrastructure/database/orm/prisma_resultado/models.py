import datetime
import decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.mysql import (
    BIGINT,
    DECIMAL,
    INTEGER,
    TEXT,
    TIMESTAMP,
    TINYINT,
    VARCHAR,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from v0_1.infrastructure.database.orm.base import Base
from v0_1.infrastructure.database.orm.prisma.investigador.models import InvestigadorORM

SCHEMA_NAME = "prisma_resultado"


class MateriaCip(Base):
    __tablename__ = "materia_cip"
    __table_args__ = {"schema": SCHEMA_NAME}

    codigo: Mapped[str] = mapped_column(String(1), primary_key=True)
    materia: Mapped[str | None] = mapped_column(String(248))


class Patente(Base):
    __tablename__ = "patente"
    __table_args__ = (
        Index("numero_solicitud", "numero_solicitud", unique=True),
        Index("titulo", "titulo", mysql_length={"titulo": 191}),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(BIGINT(20, unsigned=True), "mysql", "mariadb"),
        primary_key=True,
        autoincrement=True,
    )
    tipo: Mapped[str] = mapped_column(
        String(10).with_variant(
            VARCHAR(10, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
        server_default=text("'patente'"),
    )
    numero_solicitud: Mapped[str] = mapped_column(
        String(20).with_variant(
            VARCHAR(20, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )
    fecha_solicitud: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    titulo: Mapped[str] = mapped_column(
        String(750).with_variant(
            VARCHAR(750, charset="utf32", collation="utf32_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )
    idioma_titulo: Mapped[str] = mapped_column(
        String(2).with_variant(
            VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
        server_default=text("'es'"),
    )
    visible: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("1"),
    )
    fecha_incorporacion: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
    )
    fecha_actualizacion: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    resumen: Mapped[str | None] = mapped_column(
        Text().with_variant(
            TEXT(charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        )
    )

    dato_patente: Mapped[list["DatoPatente"]] = relationship(
        "DatoPatente", back_populates="patente"
    )
    inventor_patente: Mapped[list["InventorPatente"]] = relationship(
        "InventorPatente", back_populates="patente"
    )
    materia_patente: Mapped[list["MateriaPatente"]] = relationship(
        "MateriaPatente", back_populates="patente"
    )
    titular_patente: Mapped[list["TitularPatente"]] = relationship(
        "TitularPatente", back_populates="patente"
    )


class DatoPatente(Base):
    __tablename__ = "dato_patente"
    __table_args__ = (
        ForeignKeyConstraint(
            ["patente_id"],
            [f"{SCHEMA_NAME}.patente.id"],
            onupdate="CASCADE",
            name="dato_patente_id",
        ),
        Index("dato_patente_id", "patente_id"),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(BIGINT(20, unsigned=True), "mysql", "mariadb"),
        primary_key=True,
        autoincrement=True,
    )
    tipo: Mapped[str] = mapped_column(
        String(20).with_variant(
            VARCHAR(20, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
        comment="url, titulo_alternativo",
    )
    valor: Mapped[str] = mapped_column(
        String(250).with_variant(
            VARCHAR(250, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )
    patente_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(BIGINT(20, unsigned=True), "mysql", "mariadb"),
        nullable=False,
    )

    patente: Mapped["Patente"] = relationship("Patente", back_populates="dato_patente")


class InventorPatente(Base):
    __tablename__ = "inventor_patente"
    __table_args__ = (
        ForeignKeyConstraint(
            ["investigador_id"],
            ["prisma.i_investigador.idInvestigador"],
            onupdate="CASCADE",
            name="inventor_investigador_id",
        ),
        ForeignKeyConstraint(
            ["patente_id"],
            [f"{SCHEMA_NAME}.patente.id"],
            onupdate="CASCADE",
            name="inventor_patente_id",
        ),
        Index("inventor_investigador_id", "investigador_id"),
        Index("inventor_patente_id", "patente_id"),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(BIGINT(20, unsigned=True), "mysql", "mariadb"),
        primary_key=True,
        autoincrement=True,
    )
    patente_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(BIGINT(20, unsigned=True), "mysql", "mariadb"),
        nullable=False,
    )
    nombre: Mapped[str] = mapped_column(
        String(50).with_variant(
            VARCHAR(50, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )
    investigador_id: Mapped[int | None] = mapped_column(
        Integer().with_variant(INTEGER(10), "mysql", "mariadb")
    )

    investigador: Mapped[Optional["InvestigadorORM"]] = relationship("InvestigadorORM")
    patente: Mapped["Patente"] = relationship(
        "Patente", back_populates="inventor_patente"
    )


class MateriaPatente(Base):
    __tablename__ = "materia_patente"
    __table_args__ = (
        ForeignKeyConstraint(
            ["patente_id"],
            [f"{SCHEMA_NAME}.patente.id"],
            onupdate="CASCADE",
            name="materia_patente_id",
        ),
        Index("materia_patente_id", "patente_id"),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(BIGINT(20, unsigned=True), "mysql", "mariadb"),
        primary_key=True,
        autoincrement=True,
    )
    patente_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(BIGINT(20, unsigned=True), "mysql", "mariadb"),
        nullable=False,
    )
    clasificacion: Mapped[str] = mapped_column(
        String(3).with_variant(
            VARCHAR(3, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )
    materia: Mapped[str] = mapped_column(
        String(25).with_variant(
            VARCHAR(25, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )
    edicion: Mapped[str] = mapped_column(
        Text().with_variant(
            TEXT(charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )

    patente: Mapped["Patente"] = relationship(
        "Patente", back_populates="materia_patente"
    )


class TitularPatente(Base):
    __tablename__ = "titular_patente"
    __table_args__ = (
        ForeignKeyConstraint(
            ["patente_id"],
            [f"{SCHEMA_NAME}.patente.id"],
            onupdate="CASCADE",
            name="titular_patente_id",
        ),
        Index("patente_id", "patente_id"),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(BIGINT(20, unsigned=True), "mysql", "mariadb"),
        primary_key=True,
        autoincrement=True,
    )
    patente_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(BIGINT(20, unsigned=True), "mysql", "mariadb"),
        nullable=False,
    )
    nombre: Mapped[str] = mapped_column(
        Text().with_variant(
            TEXT(charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )
    porcentaje: Mapped[decimal.Decimal] = mapped_column(
        Numeric(5, 2).with_variant(
            DECIMAL(5, 2, unsigned=True),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )

    patente: Mapped["Patente"] = relationship(
        "Patente", back_populates="titular_patente"
    )
