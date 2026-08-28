from sqlalchemy import (
    Column,
    DateTime,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.mysql import (
    INTEGER,
    MEDIUMTEXT,
)

from v0_1.infrastructure.database.models.base import Base

t_cambios_editor = Table(
    "cambios_editor",
    Base.metadata,
    Column("responsable", String(100)),
    Column("identificador", INTEGER(11), server_default=text("'0'")),
    Column("comentario", MEDIUMTEXT),
    Column("fechaCambio", DateTime),
)


t_cambios_fuente = Table(
    "cambios_fuente",
    Base.metadata,
    Column("responsable", String(100)),
    Column("identificador", INTEGER(11), server_default=text("'0'")),
    Column("comentario", MEDIUMTEXT),
    Column("fechaCambio", DateTime),
)


t_cambios_publicacion = Table(
    "cambios_publicacion",
    Base.metadata,
    Column("responsable", String(100)),
    Column("identificador", INTEGER(11), server_default=text("'0'")),
    Column("comentario", MEDIUMTEXT),
    Column("fechaCambio", DateTime),
)
