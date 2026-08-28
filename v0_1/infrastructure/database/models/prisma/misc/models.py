import datetime

from sqlalchemy import (
    TIMESTAMP,
    Column,
    DateTime,
    Index,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.mysql import (
    INTEGER,
    TINYINT,
)
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.models.base import Base


class AConfiguracion(Base):
    __tablename__ = "a_configuracion"
    __table_args__ = {"schema": "prisma"}

    variable: Mapped[str] = mapped_column(String(25), primary_key=True)
    valor: Mapped[str] = mapped_column(String(150), nullable=False)
    editable: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("1")
    )


class AControlcambios(Base):
    __tablename__ = "a_controlcambios"
    __table_args__ = {
        Index("fechaCambio", "fechaCambio"),
        Index("identificador", "identificador"),
        Index("responsable", "responsable", "accion"),
        {"schema": "prisma"},
    }

    idCambio: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    identificador: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    comentario: Mapped[str] = mapped_column(Text, nullable=False)
    fechaCambio: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )
    responsable: Mapped[str | None] = mapped_column(String(50))
    accion: Mapped[int | None] = mapped_column(
        TINYINT(3, unsigned=True),
        comment="1 modificación de investigador, 2 modicifación publicación",
    )


class APermisos(Base):
    __tablename__ = "a_permisos"
    __table_args__ = (
        Index("uid", "uid", unique=True),
        Index("uid_2", "uid", unique=True),
        Index("uid_3", "uid", unique=True),
        {"schema": "prisma"},
    )

    idPermisos: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    uid: Mapped[str] = mapped_column(String(25), nullable=False)
    nombre: Mapped[str] = mapped_column(String(60), nullable=False)
    identificador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True),
        nullable=False,
        server_default=text("0"),
        comment="Dependerá del valor de rol.",
    )
    rol: Mapped[int | None] = mapped_column(
        TINYINT(2, unsigned=True),
        server_default=text("2"),
        comment="0: Administrador, 1:Editor de biblioteca, 2: Solo ver",
    )
    idgp: Mapped[int | None] = mapped_column(
        INTEGER(11), comment="Identificador de usuario en Gestión de Proyectos"
    )


class APermisosMultiple(Base):
    __tablename__ = "a_permisos_multiple"
    __table_args__ = {"schema": "prisma"}

    mail: Mapped[str] = mapped_column(String(100), primary_key=True)
    permiso: Mapped[str] = mapped_column(String(30), primary_key=True)


t_a_registro_cambios_editor = Table(
    "a_registro_cambios_editor",
    Base.metadata,
    Column("id", INTEGER(11), nullable=False),
    Column("id_carga", String(40), nullable=False),
    Column("tipo_dato", String(100)),
    Column("tipo_dato_2", String(100)),
    Column("tipo_dato_3", String(100)),
    Column("origen", String(100)),
    Column("valor", Text),
    Column("fecha", DateTime, nullable=False),
    Column("comentario", Text, nullable=False),
    Column("autor", String(100)),
    Index(
        "a_registro_cambios_editor_id_IDX",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
    ),
    schema="prisma",
)


t_a_registro_cambios_fuente = Table(
    "a_registro_cambios_fuente",
    Base.metadata,
    Column("id", INTEGER(11), nullable=False),
    Column("id_carga", String(40), nullable=False),
    Column("tipo_dato", String(100)),
    Column("tipo_dato_2", String(100)),
    Column("tipo_dato_3", String(100)),
    Column("origen", String(100)),
    Column("valor", Text),
    Column("valor_antiguo", String(100)),
    Column("fecha", DateTime, nullable=False),
    Column("comentario", Text, nullable=False),
    Column("autor", String(100)),
    Index(
        "a_registro_cambios_fuente_id_IDX",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
    ),
    schema="prisma",
)


t_a_registro_cambios_investigador = Table(
    "a_registro_cambios_investigador",
    Base.metadata,
    Column("id", INTEGER(11), nullable=False),
    Column("id_carga", String(40), nullable=False),
    Column("tipo_dato", String(100)),
    Column("tipo_dato_2", String(100)),
    Column("tipo_dato_3", String(100)),
    Column("origen", String(100)),
    Column("valor", Text),
    Column("valor_antiguo", String(100)),
    Column("fecha", DateTime, nullable=False),
    Column("comentario", Text, nullable=False),
    Column("autor", String(100)),
    Index(
        "a_registro_cambios_publicacion_id_IDX",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
    ),
    schema="prisma",
)


t_a_registro_cambios_publicacion = Table(
    "a_registro_cambios_publicacion",
    Base.metadata,
    Column("id", INTEGER(11), nullable=False),
    Column("id_carga", String(40), nullable=False),
    Column("tipo_dato", String(100)),
    Column("tipo_dato_2", String(100)),
    Column("tipo_dato_3", String(100)),
    Column("origen", String(100)),
    Column("valor", Text),
    Column("fecha", DateTime, nullable=False),
    Column("comentario", Text, nullable=False),
    Column("autor", String(100)),
    Column("valor_antiguo", String(100)),
    Index(
        "a_registro_cambios_publicacion_id_IDX",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
    ),
    schema="prisma",
)


t_a_registro_problemas_editor = Table(
    "a_registro_problemas_editor",
    Base.metadata,
    Column("id", INTEGER(11), nullable=False),
    Column("id_carga", String(40), nullable=False),
    Column("tipo_dato", String(100), nullable=False),
    Column("tipo_dato_2", String(100)),
    Column("tipo_dato_3", String(100)),
    Column("origen", String(100), nullable=False),
    Column("valor", Text, nullable=False),
    Column("origen_antiguo", String(100), nullable=False),
    Column("valor_antiguo", Text, nullable=False),
    Column("fecha", DateTime, nullable=False),
    Column("comentario", Text, nullable=False),
    Index(
        "a_registro_problemas_editor_id_IDX",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
        unique=True,
    ),
    schema="prisma",
)


t_a_registro_problemas_fuente = Table(
    "a_registro_problemas_fuente",
    Base.metadata,
    Column("id", INTEGER(11), nullable=False),
    Column("id_carga", String(40), nullable=False),
    Column("tipo_dato", String(100), nullable=False),
    Column("tipo_dato_2", String(100)),
    Column("tipo_dato_3", String(100)),
    Column("origen", String(100), nullable=False),
    Column("valor", Text, nullable=False),
    Column("origen_antiguo", String(100), nullable=False),
    Column("valor_antiguo", Text, nullable=False),
    Column("fecha", DateTime, nullable=False),
    Column("comentario", Text, nullable=False),
    Index(
        "a_registro_problemas_fuente_id_IDX",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
        unique=True,
    ),
    schema="prisma",
)


t_a_registro_problemas_investigador = Table(
    "a_registro_problemas_investigador",
    Base.metadata,
    Column("id", INTEGER(11), nullable=False),
    Column("id_carga", String(40), nullable=False),
    Column("tipo_dato", String(100), nullable=False),
    Column("tipo_dato_2", String(100)),
    Column("tipo_dato_3", String(100)),
    Column("origen", String(100), nullable=False),
    Column("valor", Text, nullable=False),
    Column("origen_antiguo", String(100), nullable=False),
    Column("valor_antiguo", Text, nullable=False),
    Column("fecha", DateTime, nullable=False),
    Column("comentario", Text, nullable=False),
    Index(
        "a_registro_problemas_publicacion_id_IDX",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
        unique=True,
    ),
    schema="prisma",
)


t_a_registro_problemas_publicacion = Table(
    "a_registro_problemas_publicacion",
    Base.metadata,
    Column("id", INTEGER(11), nullable=False),
    Column("id_carga", String(40), nullable=False),
    Column("tipo_dato", String(100), nullable=False),
    Column("tipo_dato_2", String(100)),
    Column("tipo_dato_3", String(100)),
    Column("origen", String(100), nullable=False),
    Column("valor", Text, nullable=False),
    Column("origen_antiguo", String(100), nullable=False),
    Column("valor_antiguo", Text, nullable=False),
    Column("fecha", DateTime, nullable=False),
    Column("comentario", Text, nullable=False),
    Index(
        "a_registro_problemas_publicacion_id_IDX",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
        unique=True,
    ),
    schema="prisma",
)


class AResponsable(Base):
    __tablename__ = "a_responsable"
    __table_args__ = (
        Index("idCentro", "centro_id"),
        Index("idPermiso", "responsable_id"),
        {"schema": "prisma"},
    )

    id: Mapped[int] = mapped_column(INTEGER(10), primary_key=True, autoincrement=True)
    responsable_id: Mapped[int | None] = mapped_column(INTEGER(10, unsigned=True))
    centro_id: Mapped[str | None] = mapped_column(String(4))
    usuario_gp_id: Mapped[int | None] = mapped_column(INTEGER(10))


t_cvn_categoria_norm = Table(
    "cvn_categoria_norm",
    Base.metadata,
    Column("id_categoria", String(6), nullable=False),
    Column("nombre", String(150), nullable=False),
    schema="prisma",
)
