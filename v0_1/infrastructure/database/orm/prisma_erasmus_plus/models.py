import datetime

from sqlalchemy import Date, Integer, SmallInteger, String, Text, text
from sqlalchemy.dialects.mysql import INTEGER, TEXT, TINYINT, VARCHAR
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.orm.base import Base

SCHEMA_NAME = "prisma_erasmus_plus"

# Generic types with MySQL/MariaDB variants
StandardInt = Integer().with_variant(INTEGER(display_width=11), "mysql", "mariadb")
TinyIntFour = SmallInteger().with_variant(TINYINT(display_width=4), "mysql", "mariadb")

Varchar20Spanish = String(20).with_variant(
    VARCHAR(20, charset="utf8mb4", collation="utf8mb4_spanish_ci"),
    "mysql",
    "mariadb",
)
Varchar255Spanish = String(255).with_variant(
    VARCHAR(255, charset="utf8mb4", collation="utf8mb4_spanish_ci"),
    "mysql",
    "mariadb",
)
TextSpanish = Text().with_variant(
    TEXT(charset="utf8mb4", collation="utf8mb4_spanish_ci"),
    "mysql",
    "mariadb",
)


class ErasmusInstituciones(Base):
    __tablename__ = "erasmus_instituciones"
    __table_args__ = {"schema": SCHEMA_NAME}

    id: Mapped[int] = mapped_column(StandardInt, primary_key=True, autoincrement=True)
    referencia: Mapped[str | None] = mapped_column(Varchar255Spanish)
    institucion: Mapped[str | None] = mapped_column(TextSpanish)
    pais: Mapped[str | None] = mapped_column(TextSpanish)
    rol: Mapped[str | None] = mapped_column(TextSpanish)


class ErasmusParticipantes(Base):
    __tablename__ = "erasmus_participantes"
    __table_args__ = {"schema": SCHEMA_NAME}

    id: Mapped[int] = mapped_column(StandardInt, primary_key=True, autoincrement=True)
    referencia: Mapped[str | None] = mapped_column(Varchar255Spanish)
    nombre: Mapped[str | None] = mapped_column(TextSpanish)
    apellido: Mapped[str | None] = mapped_column(TextSpanish)
    dni: Mapped[str | None] = mapped_column(Varchar20Spanish)
    rol: Mapped[str | None] = mapped_column(TextSpanish)


class ErasmusProyectos(Base):
    __tablename__ = "erasmus_proyectos"
    __table_args__ = {"schema": SCHEMA_NAME}

    id: Mapped[int] = mapped_column(StandardInt, primary_key=True, autoincrement=True)
    visible: Mapped[int] = mapped_column(
        TinyIntFour, nullable=False, server_default=text("1")
    )
    denominacion: Mapped[str | None] = mapped_column(TextSpanish)
    acronimo: Mapped[str | None] = mapped_column(TextSpanish)
    fecha_inicio: Mapped[datetime.date | None] = mapped_column(Date)
    fecha_fin: Mapped[datetime.date | None] = mapped_column(Date)
    entidad_financiadora: Mapped[str | None] = mapped_column(TextSpanish)
    programa_financiador: Mapped[str | None] = mapped_column(TextSpanish)
    convocatoria: Mapped[str | None] = mapped_column(TextSpanish)
    iniciativa: Mapped[str | None] = mapped_column(TextSpanish)
    importe_total_concedido: Mapped[str | None] = mapped_column(TextSpanish)
    importe_asignado_us: Mapped[str | None] = mapped_column(TextSpanish)
    referencia: Mapped[str | None] = mapped_column(Varchar255Spanish)
    web: Mapped[str | None] = mapped_column(TextSpanish)
    descripcion: Mapped[str | None] = mapped_column(TextSpanish)
    convocatoria_competitiva: Mapped[str | None] = mapped_column(TextSpanish)
    innovacion_docente: Mapped[str | None] = mapped_column(TextSpanish)
