import datetime

from sqlalchemy import TIMESTAMP, Column, Index, Integer, String, Table, Text, func
from sqlalchemy.dialects.mysql import (
    INTEGER as MYSQL_INTEGER,
)
from sqlalchemy.dialects.mysql import (
    TIMESTAMP as MYSQL_TIMESTAMP,
)
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.orm.base import Base, get_table_args

SCHEMA_NAME = "api"

# Helper type for MySQL integer with display width
MySqlInteger11 = Integer().with_variant(MYSQL_INTEGER(11), "mysql", "mariadb")


class ApiKey(Base):
    __tablename__ = "api_key"
    __table_args__ = get_table_args(
        Index("ix_api_key_api_key", "api_key", unique=True),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(
        MySqlInteger11, primary_key=True, autoincrement=True
    )
    uvus: Mapped[str] = mapped_column(String(50), nullable=False)
    api_key: Mapped[str] = mapped_column(String(32), nullable=False)
    max_requests: Mapped[int | None] = mapped_column(
        MySqlInteger11, server_default="100"
    )
    window_seconds: Mapped[int | None] = mapped_column(
        MySqlInteger11, server_default="60"
    )


class Logs(Base):
    __tablename__ = "logs"
    __table_args__ = get_table_args(
        Index("ix_logs_response_code", "response_code"),
        Index("ix_logs_route", "route"),
        Index("ix_logs_user", "user"),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(
        MySqlInteger11, primary_key=True, autoincrement=True
    )
    route: Mapped[str] = mapped_column(String(500), nullable=False)
    args: Mapped[str] = mapped_column(String(1000), nullable=False)
    response_code: Mapped[int] = mapped_column(MySqlInteger11, nullable=False)
    user: Mapped[str] = mapped_column(String(100), nullable=False)
    date: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.now()
    )


class Permisos(Base):
    __tablename__ = "permisos"
    __table_args__ = get_table_args(
        Index("uq_permisos_usuario_rol", "usuario", "rol", unique=True),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(
        MySqlInteger11, primary_key=True, autoincrement=True
    )
    usuario: Mapped[str] = mapped_column(String(200), nullable=False)
    rol: Mapped[str] = mapped_column(String(30), nullable=False)


t_permisos_endpoint = Table(
    "permisos_endpoint",
    Base.metadata,
    Column("endpoint", String(300)),
    Column("rol", String(100)),
    Column("accion", String(100)),
    Index(
        "uq_permisos_endpoint_endpoint_rol_accion",
        "endpoint",
        "rol",
        "accion",
        unique=True,
    ),
    schema=SCHEMA_NAME,
)


class Peticion(Base):
    __tablename__ = "peticion"
    __table_args__ = get_table_args(
        Index("ix_peticion_destinatario", "destinatario"),
        Index("ix_peticion_estado", "estado"),
        Index("ix_peticion_tipo", "tipo"),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[str] = mapped_column(String(52), primary_key=True)
    destinatario: Mapped[str] = mapped_column(String(200), nullable=False)
    tipo: Mapped[str] = mapped_column(String(50), nullable=False)
    parametros: Mapped[str] = mapped_column(Text, nullable=False)
    estado: Mapped[str] = mapped_column(String(50), nullable=False)
    resultado: Mapped[str] = mapped_column(Text, nullable=False)
    fecha: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.now()
    )


class SsoData(Base):
    __tablename__ = "sso_data"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    edupersonaffiliation: Mapped[str] = mapped_column(String(100), nullable=False)
    givenname: Mapped[str] = mapped_column(String(100), nullable=False)
    mail: Mapped[str] = mapped_column(String(100), nullable=False)
    schacSn1: Mapped[str] = mapped_column(String(100), nullable=False)
    schacSn2: Mapped[str] = mapped_column(String(100), nullable=False)
    schacuserstatus: Mapped[str] = mapped_column(String(100), nullable=False)
    uid: Mapped[str] = mapped_column(String(100), primary_key=True)
    usesrelacion: Mapped[str] = mapped_column(String(100), nullable=False)
    ultimo_login: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP().with_variant(MYSQL_TIMESTAMP(), "mysql", "mariadb"),
        server_default=func.now(),
        onupdate=func.now(),
    )
