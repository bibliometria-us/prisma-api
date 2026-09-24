from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Table,
    Text,
    text,
)

# Import unified Base and SCHEMA_NAME constant
from v0_1.infrastructure.database.orm.base import Base

SCHEMA_NAME = "prisma"


# ---------------------------------------------------------------------------
# Reflection Tables / Views
# ---------------------------------------------------------------------------

t_cambios_editor = Table(
    "cambios_editor",
    Base.metadata,
    Column("responsable", String(100)),
    Column("identificador", Integer, server_default=text("'0'")),
    Column("comentario", Text),
    Column("fechaCambio", DateTime),
    schema=SCHEMA_NAME,
)


t_cambios_fuente = Table(
    "cambios_fuente",
    Base.metadata,
    Column("responsable", String(100)),
    Column("identificador", Integer, server_default=text("'0'")),
    Column("comentario", Text),
    Column("fechaCambio", DateTime),
    schema=SCHEMA_NAME,
)


t_cambios_publicacion = Table(
    "cambios_publicacion",
    Base.metadata,
    Column("responsable", String(100)),
    Column("identificador", Integer, server_default=text("'0'")),
    Column("comentario", Text),
    Column("fechaCambio", DateTime),
    schema=SCHEMA_NAME,
)
