
from sqlalchemy import Index, Integer, String, text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.orm.base import Base, get_table_args

SCHEMA_NAME = "config"

# Generic type definition using with_variant:
# Uses standard Boolean cross-database, but enforces TINYINT(1) on MySQL/MariaDB
TinyIntBoolean = Integer().with_variant(TINYINT(1), "mysql", "mariadb")


class MapTiposFuenteAntiguos(Base):
    __tablename__ = "map_tipos_fuente_antiguos"
    __table_args__ = {"schema": SCHEMA_NAME}

    tipo_antiguo: Mapped[str] = mapped_column(String(100), primary_key=True)
    tipo_nuevo: Mapped[str] = mapped_column(String(100), nullable=False)


class MapTiposPublicacionAntiguos(Base):
    __tablename__ = "map_tipos_publicacion_antiguos"
    __table_args__ = {"schema": SCHEMA_NAME}

    tipo_antiguo: Mapped[str] = mapped_column(String(100), primary_key=True)
    tipo_nuevo: Mapped[str] = mapped_column(String(100), nullable=False)


class TiposFuente(Base):
    __tablename__ = "tipos_fuente"
    __table_args__ = {"schema": SCHEMA_NAME}

    nombre: Mapped[str] = mapped_column(String(100), primary_key=True)
    activo: Mapped[bool | None] = mapped_column(
        TinyIntBoolean, server_default=text("1")
    )


class TiposFuenteMap(Base):
    __tablename__ = "tipos_fuente_map"
    __table_args__ = get_table_args(
        Index(
            "idx_tipos_fuente_map_nombre_origen",
            "nombre_origen",
            "origen",
            unique=True,
        ),
        schema_name=SCHEMA_NAME,
    )

    nombre_origen: Mapped[str] = mapped_column(String(500), primary_key=True)
    nombre_prisma: Mapped[str] = mapped_column(String(100), nullable=False)
    origen: Mapped[str] = mapped_column(String(100), primary_key=True)


class TiposPublicacion(Base):
    __tablename__ = "tipos_publicacion"
    __table_args__ = get_table_args(
        Index("idx_tipos_publicacion_nombre", "nombre", unique=True),
        schema_name=SCHEMA_NAME,
    )

    nombre: Mapped[str] = mapped_column(String(100), primary_key=True)
    activo: Mapped[bool] = mapped_column(
        TinyIntBoolean, nullable=False, server_default=text("1")
    )


class TiposPublicacionMap(Base):
    __tablename__ = "tipos_publicacion_map"
    __table_args__ = get_table_args(
        Index(
            "idx_tipos_publicacion_map_nombre_origen",
            "nombre_origen",
            "origen",
            unique=True,
        ),
        schema_name=SCHEMA_NAME,
    )

    nombre_origen: Mapped[str] = mapped_column(String(500), primary_key=True)
    nombre_prisma: Mapped[str] = mapped_column(String(100), nullable=False)
    origen: Mapped[str] = mapped_column(String(100), primary_key=True)
