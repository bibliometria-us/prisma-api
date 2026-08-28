from sqlalchemy import (
    TIMESTAMP,
    Column,
    Date,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.mysql import (
    INTEGER,
    SMALLINT,
    TINYINT,
)

from v0_1.infrastructure.database.models.base import Base

t_i_investigador_activo = Table(
    "i_investigador_activo",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("nombre", String(75)),
    Column("apellidos", String(150)),
    Column("docuIden", String(45)),
    Column("email", String(75)),
    Column("idCategoria", String(8)),
    Column("fechaNombramiento", Date),
    Column("idArea", SMALLINT(3, unsigned=True, zerofill=True)),
    Column("fechaContratacion", Date),
    Column("idDepartamento", String(4)),
    Column("idCentro", String(5)),
    Column("idCentroCenso", String(5)),
    Column("sexo", TINYINT(4)),
    Column("resumen", Text, server_default=text("''''''")),
    Column("nacionalidad", String(30)),
    Column("fechaNacimiento", Date),
    Column("perfilPublico", TINYINT(1), server_default=text("'1'")),
    Column(
        "fechaActualizacion", TIMESTAMP, server_default=text("'current_timestamp()'")
    ),
    schema="prisma",
)

t_investigador_biblioteca = Table(
    "investigador_biblioteca",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("biblioteca", String(150)),
    schema="prisma",
)


t_investigador_edad = Table(
    "investigador_edad",
    Base.metadata,
    Column("id", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("apellidos", String(150)),
    Column("nombre", String(75)),
    Column("edad", String(21)),
    Column("fechaNacimiento", Date),
    Column("email", String(75)),
    schema="prisma",
)
