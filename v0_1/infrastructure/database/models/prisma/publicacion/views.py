from sqlalchemy import (
    TIMESTAMP,
    Column,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.mysql import (
    INTEGER,
    TINYINT,
)

from v0_1.infrastructure.database.models.base import Base

t_publicacionesXcentro = Table(
    "publicacionesXcentro",
    Base.metadata,
    Column("idPublicacion", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("tipo", String(50)),
    Column("titulo", String(1000)),
    Column("agno", String(4)),
    Column("idFuente", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("origen", String(50)),
    Column("validado", TINYINT(1), server_default=text("'1'")),
    Column(
        "fechaActualizacion", TIMESTAMP, server_default=text("'current_timestamp()'")
    ),
    Column("eliminado", TINYINT(1), server_default=text("'0'")),
    Column("idCentro", String(5)),
    schema="prisma",
)
