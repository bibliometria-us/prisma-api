from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    SmallInteger,
    String,
    Table,
    func,
    text,
)

# Import unified Base and SCHEMA_NAME constant
from v0_1.infrastructure.database.orm.base import Base

SCHEMA_NAME = "prisma"


# ---------------------------------------------------------------------------
# Reflection Tables / Views
# ---------------------------------------------------------------------------

t_publicacionesXcentro = Table(
    "publicacionesXcentro",
    Base.metadata,
    Column("idPublicacion", Integer, server_default=text("'0'")),
    Column("tipo", String(50)),
    Column("titulo", String(1000)),
    Column("agno", String(4)),
    Column("idFuente", Integer, server_default=text("'0'")),
    Column("origen", String(50)),
    Column("validado", SmallInteger, server_default=text("'1'")),
    Column("fechaActualizacion", DateTime, server_default=func.now()),
    Column("eliminado", SmallInteger, server_default=text("'0'")),
    Column("idCentro", String(5)),
    schema=SCHEMA_NAME,
)
