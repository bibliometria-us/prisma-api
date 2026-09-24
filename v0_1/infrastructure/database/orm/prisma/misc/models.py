import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    SmallInteger,
    String,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.mysql import TIMESTAMP, TINYINT
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.orm.base import Base, get_table_args

SCHEMA_NAME = "prisma"


# ---------------------------------------------------------------------------
# Declarative ORM Models
# ---------------------------------------------------------------------------


class AConfiguracionORM(Base):
    __tablename__ = "a_configuracion"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    variable: Mapped[str] = mapped_column(String(25), primary_key=True)
    valor: Mapped[str] = mapped_column(String(150), nullable=False)
    editable: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("1"),
    )


class AControlcambiosORM(Base):
    __tablename__ = "a_controlcambios"
    __table_args__ = get_table_args(
        Index("idx_acc_fechaCambio", "fechaCambio"),
        Index("idx_acc_identificador", "identificador"),
        Index("idx_acc_responsable_accion", "responsable", "accion"),
        schema_name=SCHEMA_NAME,
    )

    idCambio: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    identificador: Mapped[int] = mapped_column(Integer, nullable=False)
    comentario: Mapped[str] = mapped_column(Text, nullable=False)
    fechaCambio: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
    )
    responsable: Mapped[str | None] = mapped_column(String(50))
    accion: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(TINYINT(3, unsigned=True), "mysql", "mariadb"),
        comment="1 modificación de investigador, 2 modicifación publicación",
    )


class APermisosORM(Base):
    __tablename__ = "a_permisos"
    __table_args__ = get_table_args(
        Index("idx_ap_uid_unique", "uid", unique=True), schema_name=SCHEMA_NAME
    )

    idPermisos: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    uid: Mapped[str] = mapped_column(String(25), nullable=False)
    nombre: Mapped[str] = mapped_column(String(60), nullable=False)
    identificador: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
        comment="Dependerá del valor de rol.",
    )
    rol: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(TINYINT(2, unsigned=True), "mysql", "mariadb"),
        server_default=text("2"),
        comment="0: Administrador, 1:Editor de biblioteca, 2: Solo ver",
    )
    idgp: Mapped[int | None] = mapped_column(
        Integer, comment="Identificador de usuario en Gestión de Proyectos"
    )


class APermisosMultipleORM(Base):
    __tablename__ = "a_permisos_multiple"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    mail: Mapped[str] = mapped_column(String(100), primary_key=True)
    permiso: Mapped[str] = mapped_column(String(30), primary_key=True)


class AResponsableORM(Base):
    __tablename__ = "a_responsable"
    __table_args__ = get_table_args(
        Index("idx_ar_centro", "centro_id"),
        Index("idx_ar_responsable", "responsable_id"),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    responsable_id: Mapped[int | None] = mapped_column(Integer)
    centro_id: Mapped[str | None] = mapped_column(String(4))
    usuario_gp_id: Mapped[int | None] = mapped_column(Integer)


# ---------------------------------------------------------------------------
# Reflection Tables / Views
# ---------------------------------------------------------------------------

t_a_registro_cambios_editor = Table(
    "a_registro_cambios_editor",
    Base.metadata,
    Column("id", Integer, nullable=False),
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
        "idx_arce_id_combo",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
    ),
    schema=SCHEMA_NAME,
)


t_a_registro_cambios_fuente = Table(
    "a_registro_cambios_fuente",
    Base.metadata,
    Column("id", Integer, nullable=False),
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
        "idx_arcf_id_combo",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
    ),
    schema=SCHEMA_NAME,
)


t_a_registro_cambios_investigador = Table(
    "a_registro_cambios_investigador",
    Base.metadata,
    Column("id", Integer, nullable=False),
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
        "idx_arci_id_combo",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
    ),
    schema=SCHEMA_NAME,
)


t_a_registro_cambios_publicacion = Table(
    "a_registro_cambios_publicacion",
    Base.metadata,
    Column("id", Integer, nullable=False),
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
        "idx_arcp_id_combo",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
    ),
    schema=SCHEMA_NAME,
)


t_a_registro_problemas_editor = Table(
    "a_registro_problemas_editor",
    Base.metadata,
    Column("id", Integer, nullable=False),
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
        "idx_arpe_id_combo_unique",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
        unique=True,
    ),
    schema=SCHEMA_NAME,
)


t_a_registro_problemas_fuente = Table(
    "a_registro_problemas_fuente",
    Base.metadata,
    Column("id", Integer, nullable=False),
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
        "idx_arpf_id_combo_unique",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
        unique=True,
    ),
    schema=SCHEMA_NAME,
)


t_a_registro_problemas_investigador = Table(
    "a_registro_problemas_investigador",
    Base.metadata,
    Column("id", Integer, nullable=False),
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
        "idx_arpi_id_combo_unique",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
        unique=True,
    ),
    schema=SCHEMA_NAME,
)


t_a_registro_problemas_publicacion = Table(
    "a_registro_problemas_publicacion",
    Base.metadata,
    Column("id", Integer, nullable=False),
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
        "idx_arpp_id_combo_unique",
        "id",
        "tipo_dato",
        "tipo_dato_2",
        "tipo_dato_3",
        unique=True,
    ),
    schema=SCHEMA_NAME,
)


t_cvn_categoria_norm = Table(
    "cvn_categoria_norm",
    Base.metadata,
    Column("id_categoria", String(6), nullable=False),
    Column("nombre", String(150), nullable=False),
    schema=SCHEMA_NAME,
)
