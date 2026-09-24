from sqlalchemy import (
    TIMESTAMP,
    Column,
    Date,
    Integer,
    SmallInteger,
    String,
    Table,
    Text,
    func,
    text,
)

from v0_1.infrastructure.database.orm.base import Base

t_i_investigador_activo = Table(
    "i_investigador_activo",
    Base.metadata,
    Column("idInvestigador", Integer),
    Column("nombre", String(75)),
    Column("apellidos", String(150)),
    Column("docuIden", String(45)),
    Column("email", String(75)),
    Column("idCategoria", String(8)),
    Column("fechaNombramiento", Date),
    Column("idArea", SmallInteger),
    Column("fechaContratacion", Date),
    Column("idDepartamento", String(4)),
    Column("idCentro", String(5)),
    Column("idCentroCenso", String(5)),
    Column("sexo", SmallInteger),
    Column("resumen", Text, server_default=text("''''''")),
    Column("nacionalidad", String(30)),
    Column("fechaNacimiento", Date),
    Column("perfilPublico", SmallInteger, server_default=text("'1'")),
    Column("fechaActualizacion", TIMESTAMP, server_default=func.current_timestamp()),
    schema="prisma",
)

t_investigador_biblioteca = Table(
    "investigador_biblioteca",
    Base.metadata,
    Column("idInvestigador", Integer),
    Column("biblioteca", String(150)),
    schema="prisma",
)


t_investigador_edad = Table(
    "investigador_edad",
    Base.metadata,
    Column("id", Integer),
    Column("apellidos", String(150)),
    Column("nombre", String(75)),
    Column("edad", String(21)),
    Column("fechaNacimiento", Date),
    Column("email", String(75)),
    schema="prisma",
)
