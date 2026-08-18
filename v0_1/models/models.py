from typing import Optional
import datetime
import decimal

from sqlalchemy import (
    Column,
    DECIMAL,
    Date,
    DateTime,
    Float,
    Index,
    String,
    TIMESTAMP,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.mysql import (
    BIGINT,
    INTEGER,
    MEDIUMTEXT,
    SMALLINT,
    TINYINT,
    VARCHAR,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


t_TABLE_115 = Table(
    "TABLE 115",
    Base.metadata,
    Column("Editorial", String(21)),
    Column("ISSN", String(11)),
    Column("eISSN", String(10)),
    Column("Título", String(122)),
    Column("Tipo", String(22)),
    Column("Descuento", String(94)),
    Column("Promotor", String(13)),
    Column("idFuente", INTEGER(10)),
)


class AControlcambios(Base):
    __tablename__ = "a_controlcambios"
    __table_args__ = (
        Index("fechaCambio", "fechaCambio"),
        Index("identificador", "identificador"),
        Index("responsable", "responsable", "accion"),
    )

    idCambio: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    identificador: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    comentario: Mapped[str] = mapped_column(Text, nullable=False)
    fechaCambio: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )
    responsable: Mapped[Optional[str]] = mapped_column(String(50))
    accion: Mapped[Optional[int]] = mapped_column(
        TINYINT(3, unsigned=True),
        comment="1 modificación de investigador, 2 modicifación publicación",
    )


class ANota(Base):
    __tablename__ = "a_nota"
    __table_args__ = (Index("tipo", "tipo", "elemento_id", unique=True),)

    id: Mapped[int] = mapped_column(
        BIGINT(20, unsigned=True), primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(String(20), nullable=False)
    elemento_id: Mapped[int] = mapped_column(BIGINT(20, unsigned=True), nullable=False)
    valor: Mapped[str] = mapped_column(Text, nullable=False)


class APermisos(Base):
    __tablename__ = "a_permisos"
    __table_args__ = (
        Index("uid", "uid", unique=True),
        Index("uid_2", "uid", unique=True),
        Index("uid_3", "uid", unique=True),
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
    rol: Mapped[Optional[int]] = mapped_column(
        TINYINT(2, unsigned=True),
        server_default=text("2"),
        comment="0: Administrador, 1:Editor de biblioteca, 2: Solo ver",
    )
    idgp: Mapped[Optional[int]] = mapped_column(
        INTEGER(11), comment="Identificador de usuario en Gestión de Proyectos"
    )


class APermisosMultiple(Base):
    __tablename__ = "a_permisos_multiple"

    mail: Mapped[str] = mapped_column(String(100), primary_key=True)
    permiso: Mapped[str] = mapped_column(String(30), primary_key=True)


t_a_problemas = Table(
    "a_problemas",
    Base.metadata,
    Column("idCarga", String(40), nullable=False),
    Column("tipo_problema", String(100), nullable=False),
    Column("tipo_dato", String(100), nullable=False),
    Column("id_dato", String(100), nullable=False),
    Column("mensaje", Text, nullable=False),
    Column("tipo_dato_2", String(100)),
    Column("tipo_dato_3", String(100)),
    Column("antigua_fuente", String(100)),
    Column("antiguo_valor", String(100), nullable=False),
    Column("nueva_fuente", String(100), nullable=False),
    Column("nuevo_valor", String(100), nullable=False),
)


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
)


class AResponsable(Base):
    __tablename__ = "a_responsable"
    __table_args__ = (
        Index("idCentro", "centro_id"),
        Index("idPermiso", "responsable_id"),
    )

    id: Mapped[int] = mapped_column(INTEGER(10), primary_key=True, autoincrement=True)
    responsable_id: Mapped[Optional[int]] = mapped_column(INTEGER(10, unsigned=True))
    centro_id: Mapped[Optional[str]] = mapped_column(String(4))
    usuario_gp_id: Mapped[Optional[int]] = mapped_column(INTEGER(10))


class ATareaPendiente(Base):
    __tablename__ = "a_tarea_pendiente"
    __table_args__ = (
        Index("fechaResuelto", "fechaResuelto"),
        Index("uid", "responsable", "tipo"),
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    responsable: Mapped[str] = mapped_column(String(20), nullable=False)
    tipo: Mapped[str] = mapped_column(String(30), nullable=False)
    valor: Mapped[str] = mapped_column(String(500), nullable=False)
    email: Mapped[str] = mapped_column(String(40), nullable=False)
    fechaEntrada: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )
    respuesta: Mapped[Optional[int]] = mapped_column(TINYINT(4))
    fechaResuelto: Mapped[Optional[datetime.datetime]] = mapped_column(TIMESTAMP)


t_aprobadas_2023 = Table(
    "aprobadas_2023",
    Base.metadata,
    Column("ID_Aprobada", INTEGER(3)),
    Column("Fecha", String(10)),
    Column("Mes", String(10)),
    Column("Editorial", String(10)),
    Column("Nombre", String(20)),
    Column("Apellidos", String(27)),
    Column("Correo", String(23)),
    Column("Figura", String(45)),
    Column("Figura (Agrupada)", String(43)),
    Column("Grupo", String(9)),
    Column("Género", String(9)),
    Column("ID Prisma", String(4)),
    Column("Otro autor", String(35)),
    Column("Figura Otro autor", String(33)),
    Column("ID Prisma Otro autor", String(4)),
    Column("Departamento", String(65)),
    Column("Área de conocimiento", String(52)),
    Column("Biblioteca", String(22)),
    Column("Centro", String(42)),
    Column("Ramas de conocimiento", String(27)),
    Column("Tipo de revista", String(11)),
    Column("Tipo de artículo", String(29)),
    Column("DOI", String(36)),
    Column("CC", String(11)),
    Column("ID_Revista", INTEGER(5)),
    Column("Revista", String(90)),
    Column("ISSN", String(9)),
    Column("eISSN", String(9)),
    Column("Título", String(224)),
    Column("URL", String(55)),
    Column("Precio", String(10)),
    Column("Descuento", String(8)),
    Column("APC", String(10)),
    Column("JIF Mejor Cuartil", String(6)),
    Column("CiteScore Mejor cuartil", String(12)),
)


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


t_cvn_categoria_norm = Table(
    "cvn_categoria_norm",
    Base.metadata,
    Column("id_categoria", String(6), nullable=False),
    Column("nombre", String(150), nullable=False),
)


class CvnDescarga(Base):
    __tablename__ = "cvn_descarga"
    __table_args__ = (Index("fechaDescarga", "fechaDescarga"),)

    idDescarga: Mapped[int] = mapped_column(
        INTEGER(10), primary_key=True, autoincrement=True
    )
    responsable: Mapped[str] = mapped_column(String(50), nullable=False)
    tipo: Mapped[int] = mapped_column(
        INTEGER(1), nullable=False, comment=" 1 descarga CVN, 2 descarga CVA "
    )
    identificador: Mapped[int] = mapped_column(INTEGER(22), nullable=False)
    fechaDescarga: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )


t_eliminados_scholar = Table(
    "eliminados_scholar",
    Base.metadata,
    Column("biblioteca", String(150)),
    Column("fechaEliminacion", TIMESTAMP, server_default=text("'current_timestamp()'")),
    Column("idInvestigador", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("nombre", String(75)),
    Column("apellidos", String(150)),
    Column("docuIden", String(45)),
    Column("email", String(75)),
    Column("idCategoria", String(8)),
    Column("idArea", SMALLINT(3, unsigned=True, zerofill=True)),
    Column("fechaContratacion", Date),
    Column("idDepartamento", String(4)),
    Column("idCentro", String(5)),
    Column("nacionalidad", String(30)),
    Column("sexo", TINYINT(4)),
    Column("fechaNacimiento", Date),
    Column("fechaNombramiento", Date),
    Column("perfilPublico", TINYINT(1), server_default=text("'1'")),
    Column(
        "fechaActualizacion", TIMESTAMP, server_default=text("'current_timestamp()'")
    ),
)


class IArea(Base):
    __tablename__ = "i_area"
    __table_args__ = (
        Index("idxRama", "idRama"),
        Index("nombre_UNIQUE", "nombre", unique=True),
    )

    idArea: Mapped[int] = mapped_column(
        SMALLINT(3, unsigned=True, zerofill=True), primary_key=True
    )
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    idRama: Mapped[int] = mapped_column(TINYINT(3, unsigned=True), nullable=False)


class IBiblioteca(Base):
    __tablename__ = "i_biblioteca"

    idBiblioteca: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), primary_key=True
    )
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)


class ICategoria(Base):
    __tablename__ = "i_categoria"
    __table_args__ = (
        Index("femenino", "femenino"),
        Index("idCategoria", "idCategoria"),
        Index("nombre", "nombre"),
    )

    idCategoria: Mapped[str] = mapped_column(
        String(8), primary_key=True, server_default=text("''")
    )
    tipo_pp: Mapped[str] = mapped_column(
        VARCHAR(3, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
        nullable=False,
        server_default=text("'exc'"),
        comment="Tipo de usuario para los informes del plan propio. exc Excluido, cat Catedrático, mie Miembro, pre Predoctoral, pos Postdoctoral",
    )
    nombre: Mapped[Optional[str]] = mapped_column(String(50))
    femenino: Mapped[Optional[str]] = mapped_column(String(50))


class ICentro(Base):
    __tablename__ = "i_centro"
    __table_args__ = (
        Index("biblioteca_idx", "idBiblioteca"),
        Index("nombre", "nombre"),
    )

    idCentro: Mapped[str] = mapped_column(String(5), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    idBiblioteca: Mapped[Optional[int]] = mapped_column(
        TINYINT(2, unsigned=True), server_default=text("0")
    )
    encargado: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class ICentroMixto(Base):
    __tablename__ = "i_centro_mixto"

    idCentroMixto: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(500), nullable=False)
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    acronimo: Mapped[Optional[str]] = mapped_column(String(20))
    ambito: Mapped[Optional[str]] = mapped_column(String(100))
    url: Mapped[Optional[str]] = mapped_column(Text)
    fecha_creacion: Mapped[Optional[datetime.date]] = mapped_column(Date)


class ICentroMixtoLineaInvestigacion(Base):
    __tablename__ = "i_centro_mixto_linea_investigacion"

    idCentroMixto: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class ICentroMixtoPalabraClave(Base):
    __tablename__ = "i_centro_mixto_palabra_clave"

    idCentroMixto: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class IConjunto(Base):
    __tablename__ = "i_conjunto"
    __table_args__ = (Index("tipo", "tipo"),)

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    tipo: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="Instituto, Unidad de Excelencia, etc."
    )
    nombre: Mapped[str] = mapped_column(
        VARCHAR(500, charset="utf8mb4", collation="utf8mb4_spanish_ci"), nullable=False
    )
    acronimo: Mapped[Optional[str]] = mapped_column(
        VARCHAR(20, charset="utf8mb4", collation="utf8mb4_spanish_ci"),
        comment="Nombre acortado",
    )
    responsable_id: Mapped[Optional[int]] = mapped_column(
        INTEGER(11), comment="Identificador del investigador responsable"
    )


class IDepartamento(Base):
    __tablename__ = "i_departamento"
    __table_args__ = (Index("nombre", "nombre"),)

    idDepartamento: Mapped[str] = mapped_column(String(4), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)


class IDoctorado(Base):
    __tablename__ = "i_doctorado"
    __table_args__ = (Index("nombre", "nombre", mysql_length={"nombre": 191}),)

    idDoctorado: Mapped[int] = mapped_column(
        SMALLINT(4), primary_key=True, server_default=text("0")
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    nombre: Mapped[Optional[str]] = mapped_column(
        VARCHAR(250, charset="utf8mb4", collation="utf8mb4_spanish_ci")
    )


class IDoctoradoLineaInvestigacion(Base):
    __tablename__ = "i_doctorado_linea_investigacion"

    idDoctorado: Mapped[int] = mapped_column(INTEGER(4), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(INTEGER(8), primary_key=True)


class IFechaCese(Base):
    __tablename__ = "i_fecha_cese"
    __table_args__ = (
        Index("fk_motivocese_has_investigador_investigador1_idx", "idInvestigador"),
        Index("fk_motivocese_has_investigador_motivocese1_idx", "idMotivo"),
        Index("idInves_UNIQUE", "idInvestigador", unique=True),
    )

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True
    )
    idMotivo: Mapped[str] = mapped_column(String(8), primary_key=True)
    fechaCese: Mapped[datetime.date] = mapped_column(Date, nullable=False)


class IGrupo(Base):
    __tablename__ = "i_grupo"
    __table_args__ = (
        Index("acronimo", "acronimo"),
        Index("nombre", "nombre", mysql_length={"nombre": 191}),
    )

    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    ambito: Mapped[str] = mapped_column(
        String(100), nullable=False, server_default=text("'Andalucía'")
    )
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    acronimo: Mapped[Optional[str]] = mapped_column(
        String(75), comment="Acrónimo del grupo (viene de SISIUS)"
    )
    rama: Mapped[Optional[str]] = mapped_column(
        String(4),
        comment="Rama cientifíca en la que se engloba el grupo de investigación (viene de SISISUS)",
    )
    codigo: Mapped[Optional[int]] = mapped_column(SMALLINT(4))
    institucion: Mapped[Optional[str]] = mapped_column(String(200))
    fecha_creacion: Mapped[Optional[datetime.date]] = mapped_column(Date)
    estado: Mapped[Optional[str]] = mapped_column(String(100))
    situacion: Mapped[Optional[str]] = mapped_column(String(100))


class IGrupoInvestigador(Base):
    __tablename__ = "i_grupo_investigador"
    __table_args__ = (Index("idInvestigador", "idInvestigador", unique=True),)

    idInvestigador: Mapped[int] = mapped_column(INTEGER(10), primary_key=True)
    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    rol: Mapped[Optional[str]] = mapped_column(
        String(50), server_default=text("'Miembro'")
    )
    actualizado: Mapped[Optional[datetime.datetime]] = mapped_column(TIMESTAMP)


class IGrupoLineaInvestigacion(Base):
    __tablename__ = "i_grupo_linea_investigacion"

    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class IGrupoPalabraClave(Base):
    __tablename__ = "i_grupo_palabra_clave"
    __table_args__ = (
        Index("idGrupo", "idGrupo"),
        Index("idPalabraClave", "idPalabraClave"),
    )

    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class IIdentificadorInvestigador(Base):
    __tablename__ = "i_identificador_investigador"
    __table_args__ = (
        Index("investigador_idx", "idInvestigador"),
        Index("tipo", "tipo"),
        Index("tipo_2", "tipo", "valor", unique=True),
        Index("valor", "valor"),
    )

    idIdentificador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False
    )
    tipo: Mapped[str] = mapped_column(
        String(45),
        nullable=False,
        comment="orcid, wos, scopus, dialnet, idus, scholar\n",
    )
    valor: Mapped[str] = mapped_column(String(100), nullable=False)
    comentario: Mapped[Optional[str]] = mapped_column(String(200))
    eliminado: Mapped[Optional[int]] = mapped_column(
        TINYINT(1), server_default=text("0")
    )


class IIdentificadorWos(Base):
    __tablename__ = "i_identificador_wos"
    __table_args__ = (
        Index("investigador_idx", "investigador_id"),
        Index("tipo_2", "valor", unique=True),
        Index("valor", "valor"),
    )

    id: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    investigador_id: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False
    )
    valor: Mapped[str] = mapped_column(String(100), nullable=False)


class IInstitucionColectivo(Base):
    __tablename__ = "i_institucion_colectivo"

    idInstitucion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idColectivo: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    tipo: Mapped[str] = mapped_column(String(50), primary_key=True)


class IInstituto(Base):
    __tablename__ = "i_instituto"
    __table_args__ = (
        Index("acronimo", "acronimo"),
        Index("nombre", "nombre", mysql_length={"nombre": 191}),
    )

    idInstituto: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    acronimo: Mapped[str] = mapped_column(String(20), nullable=False)
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    ambito: Mapped[Optional[str]] = mapped_column(String(100))
    url: Mapped[Optional[str]] = mapped_column(Text)
    fecha_creacion: Mapped[Optional[datetime.date]] = mapped_column(Date)


class IInstitutoLineaInvestigacion(Base):
    __tablename__ = "i_instituto_linea_investigacion"

    idInstituto: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class IInstitutoPalabraClave(Base):
    __tablename__ = "i_instituto_palabra_clave"

    idInstituto: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class IInvestigador(Base):
    __tablename__ = "i_investigador"
    __table_args__ = (
        Index("apellidos", "apellidos"),
        Index("docuIden", "docuIden", unique=True),
        Index("docuIden_UNIQUE", "docuIden", unique=True),
        Index("email_UNIQUE", "email", unique=True),
        Index("i_investigador_idCentroCenso_IDX", "idCentroCenso"),
        Index("idxArea", "idArea"),
        Index("idxCategoria", "idCategoria"),
        Index("idxCentro", "idCentro"),
        Index("idxDepartamento", "idDepartamento"),
        Index("nombre", "nombre"),
    )

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True),
        primary_key=True,
        autoincrement=True,
        comment="Identificador del investigador dentro de la base de datos de bibliometría",
    )
    nombre: Mapped[str] = mapped_column(
        String(75), nullable=False, comment="Nombre propio del investigador"
    )
    apellidos: Mapped[str] = mapped_column(
        String(150), nullable=False, comment="Apellido o apellidos del investigador"
    )
    docuIden: Mapped[str] = mapped_column(
        String(45),
        nullable=False,
        comment="Documento de identidad (dni, pasaporte, etc.)",
    )
    idCategoria: Mapped[str] = mapped_column(String(8), nullable=False)
    idArea: Mapped[int] = mapped_column(
        SMALLINT(3, unsigned=True, zerofill=True), nullable=False
    )
    idDepartamento: Mapped[str] = mapped_column(String(4), nullable=False)
    idCentro: Mapped[str] = mapped_column(String(5), nullable=False)
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    perfilPublico: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("1")
    )
    email: Mapped[Optional[str]] = mapped_column(
        String(75),
        comment="Correo electrónico del investigador. Debería ser del dominio us.es",
    )
    fechaContratacion: Mapped[Optional[datetime.date]] = mapped_column(
        Date, comment="Primera fecha de contratación"
    )
    idCentroCenso: Mapped[Optional[str]] = mapped_column(String(5))
    nacionalidad: Mapped[Optional[str]] = mapped_column(String(30))
    sexo: Mapped[Optional[int]] = mapped_column(TINYINT(4))
    fechaNacimiento: Mapped[Optional[datetime.date]] = mapped_column(Date)
    fechaNombramiento: Mapped[Optional[datetime.date]] = mapped_column(Date)
    fechaActualizacion: Mapped[Optional[datetime.datetime]] = mapped_column(
        TIMESTAMP,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
        comment="Fecha de la última actualización del registro",
    )


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
)


class IInvestigadorExcluido(Base):
    __tablename__ = "i_investigador_excluido"
    __table_args__ = (Index("excluido", "excluido"),)

    idInvestigador: Mapped[int] = mapped_column(INTEGER(10), primary_key=True)
    excluido: Mapped[Optional[int]] = mapped_column(
        TINYINT(1), comment="0: Siempre admitido. 1: Siempre excluido"
    )


class IInvestigadorLineaInvestigacion(Base):
    __tablename__ = "i_investigador_linea_investigacion"

    idInvestigador: Mapped[int] = mapped_column(INTEGER(10), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class IInvestigadorPalabraClave(Base):
    __tablename__ = "i_investigador_palabra_clave"

    idInvestigador: Mapped[int] = mapped_column(INTEGER(10), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class ILineaInvestigacion(Base):
    __tablename__ = "i_linea_investigacion"

    idLineaInvestigacion: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(Text, nullable=False)
    fecha: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )


class ILineaInvestigacionDoctorado(Base):
    __tablename__ = "i_linea_investigacion_doctorado"

    idLineaInvestigacion: Mapped[int] = mapped_column(INTEGER(8), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(500), nullable=False)


class IMiembroCentroMixto(Base):
    __tablename__ = "i_miembro_centro_mixto"
    __table_args__ = (
        Index("idxInstituto", "idInvestigador"),
        Index("idxInvestigador", "idCentroMixto"),
        Index("rol", "rol"),
    )

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True
    )
    idCentroMixto: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), nullable=False
    )
    rol: Mapped[str] = mapped_column(
        String(100), nullable=False, server_default=text("'Miembro ordinario'")
    )
    actualizado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("1")
    )


class IMiembroConjunto(Base):
    __tablename__ = "i_miembro_conjunto"

    investigador_id: Mapped[int] = mapped_column(INTEGER(11), primary_key=True)
    conjunto_id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)


class IMiembroInstituto(Base):
    __tablename__ = "i_miembro_instituto"
    __table_args__ = (
        Index("idxInstituto", "idInvestigador"),
        Index("idxInvestigador", "idInstituto"),
        Index("rol", "rol"),
    )

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True
    )
    idInstituto: Mapped[int] = mapped_column(TINYINT(2, unsigned=True), nullable=False)
    rol: Mapped[str] = mapped_column(
        String(100), nullable=False, server_default=text("'Miembro ordinario'")
    )
    actualizado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("1")
    )


class IMiembroInstitutoConjunto(Base):
    __tablename__ = "i_miembro_instituto_conjunto"
    __table_args__ = (
        Index("idxInstituto", "idInvestigador"),
        Index("idxInvestigador", "idInstituto"),
        Index("rol", "rol"),
    )

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True
    )
    idInstituto: Mapped[int] = mapped_column(TINYINT(2, unsigned=True), nullable=False)
    rol: Mapped[str] = mapped_column(
        String(100), nullable=False, server_default=text("'Miembro ordinario'")
    )
    actualizado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("1")
    )


class IMiembroUnidadExcelencia(Base):
    __tablename__ = "i_miembro_unidad_excelencia"
    __table_args__ = (
        Index("idInvestigador", "idInvestigador"),
        Index("idxInstituto", "idInvestigador"),
        Index("idxInvestigador", "idUdExcelencia"),
        Index("rol", "rol"),
    )

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True
    )
    idUdExcelencia: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), nullable=False
    )
    rol: Mapped[str] = mapped_column(
        String(100), nullable=False, server_default=text("'Miembro ordinario'")
    )
    actualizado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("1")
    )


class IMotivoCese(Base):
    __tablename__ = "i_motivo_cese"

    idMotivo: Mapped[str] = mapped_column(String(8), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(250), nullable=False)


class IPalabraClave(Base):
    __tablename__ = "i_palabra_clave"

    idPalabraClave: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    fecha: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )


class IProfesorDoctorado(Base):
    __tablename__ = "i_profesor_doctorado"

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10), primary_key=True, server_default=text("0")
    )
    idDoctorado: Mapped[int] = mapped_column(
        SMALLINT(4), primary_key=True, server_default=text("0")
    )
    actualizado: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )


class IProfesorDoctorado20231201(Base):
    __tablename__ = "i_profesor_doctorado_20231201"

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10), primary_key=True, server_default=text("0")
    )
    idDoctorado: Mapped[int] = mapped_column(
        SMALLINT(4), primary_key=True, server_default=text("0")
    )
    actualizado: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )


t_i_profesor_doctorado_202312011120 = Table(
    "i_profesor_doctorado_202312011120",
    Base.metadata,
    Column("idInvestigador", INTEGER(10), nullable=False, server_default=text("0")),
    Column("idDoctorado", SMALLINT(4), nullable=False, server_default=text("0")),
    Column(
        "actualizado",
        TIMESTAMP,
        nullable=False,
        server_default=text("'0000-00-00 00:00:00'"),
    ),
)


t_i_profesor_doctorado_202312011121 = Table(
    "i_profesor_doctorado_202312011121",
    Base.metadata,
    Column("idInvestigador", INTEGER(10), nullable=False, server_default=text("0")),
    Column("idDoctorado", SMALLINT(4), nullable=False, server_default=text("0")),
    Column(
        "actualizado",
        TIMESTAMP,
        nullable=False,
        server_default=text("'0000-00-00 00:00:00'"),
    ),
)


class IProfesorDoctoradoLineaInv(Base):
    __tablename__ = "i_profesor_doctorado_linea_inv"

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10), primary_key=True, server_default=text("0")
    )
    idLineaInvestigacion: Mapped[int] = mapped_column(
        INTEGER(8), primary_key=True, server_default=text("0")
    )
    actualizado: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )


class IRama(Base):
    __tablename__ = "i_rama"
    __table_args__ = (
        Index("fk_i_rama_i_rama1_idx", "padre"),
        Index("nombre_UNIQUE", "nombre", unique=True),
    )

    idRama: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    padre: Mapped[Optional[int]] = mapped_column(
        TINYINT(2, unsigned=True),
        server_default=text("0"),
        comment="0 cuando es una rama fundamental",
    )


class IRamaUs(Base):
    __tablename__ = "i_rama_us"

    idDepartamento: Mapped[str] = mapped_column(String(4), primary_key=True)
    idArea: Mapped[int] = mapped_column(
        SMALLINT(3, unsigned=True, zerofill=True), primary_key=True
    )
    idRama: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), primary_key=True, server_default=text("0")
    )


class IRamaUs20230601(Base):
    __tablename__ = "i_rama_us_20230601"

    idDepartamento: Mapped[str] = mapped_column(String(4), primary_key=True)
    idArea: Mapped[int] = mapped_column(
        SMALLINT(3, unsigned=True, zerofill=True), primary_key=True
    )
    idRama: Mapped[int] = mapped_column(
        TINYINT(3, unsigned=True), primary_key=True, server_default=text("0")
    )


class IRamaUsNueva(Base):
    __tablename__ = "i_rama_us_nueva"

    idDepartamento: Mapped[str] = mapped_column(String(4), primary_key=True)
    idArea: Mapped[int] = mapped_column(
        SMALLINT(3, unsigned=True, zerofill=True), primary_key=True
    )
    idRama: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), primary_key=True, server_default=text("0")
    )


class IRamaUsNueva20230601(Base):
    __tablename__ = "i_rama_us_nueva_20230601"

    idDepartamento: Mapped[str] = mapped_column(String(4), primary_key=True)
    idArea: Mapped[int] = mapped_column(
        SMALLINT(3, unsigned=True, zerofill=True), primary_key=True
    )
    idRama: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), primary_key=True, server_default=text("0")
    )
    Departamento: Mapped[Optional[str]] = mapped_column(
        VARCHAR(99, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    Área: Mapped[Optional[str]] = mapped_column(
        VARCHAR(58, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    Rama: Mapped[Optional[str]] = mapped_column(
        VARCHAR(29, charset="utf8mb3", collation="utf8mb3_general_ci")
    )


class ISexenio(Base):
    __tablename__ = "i_sexenio"
    __table_args__ = (
        Index("inicio", "inicio", "fin"),
        Index("investigador_id", "investigador_id"),
        Index(
            "investigador_id_2",
            "investigador_id",
            "inicio",
            "fin",
            "transferencia",
            unique=True,
        ),
        Index("transferencia", "transferencia"),
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    investigador_id: Mapped[int] = mapped_column(BIGINT(20), nullable=False)
    inicio: Mapped[str] = mapped_column(String(4), nullable=False)
    fin: Mapped[str] = mapped_column(String(4), nullable=False)
    transferencia: Mapped[int] = mapped_column(TINYINT(1), nullable=False)
    entrada_vigor: Mapped[str] = mapped_column(String(4), nullable=False)
    id_categoria: Mapped[str] = mapped_column(String(8), nullable=False)
    nomina: Mapped[int] = mapped_column(TINYINT(1), nullable=False)


class ISexenio20230602(Base):
    __tablename__ = "i_sexenio_20230602"
    __table_args__ = (
        Index("inicio", "inicio", "fin"),
        Index("investigador_id", "investigador_id"),
        Index("transferencia", "transferencia"),
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    investigador_id: Mapped[int] = mapped_column(BIGINT(20), nullable=False)
    inicio: Mapped[str] = mapped_column(String(4), nullable=False)
    fin: Mapped[str] = mapped_column(String(4), nullable=False)
    transferencia: Mapped[int] = mapped_column(TINYINT(1), nullable=False)


class ISexenio20230921(Base):
    __tablename__ = "i_sexenio_20230921"
    __table_args__ = (
        Index("inicio", "inicio", "fin"),
        Index("investigador_id", "investigador_id"),
        Index("transferencia", "transferencia"),
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    investigador_id: Mapped[int] = mapped_column(BIGINT(20), nullable=False)
    inicio: Mapped[str] = mapped_column(String(4), nullable=False)
    fin: Mapped[str] = mapped_column(String(4), nullable=False)
    transferencia: Mapped[int] = mapped_column(TINYINT(1), nullable=False)


class IUnidadExcelencia(Base):
    __tablename__ = "i_unidad_excelencia"

    idUdExcelencia: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(
        VARCHAR(500, charset="utf8mb4", collation="utf8mb4_spanish_ci"), nullable=False
    )
    resumen: Mapped[str] = mapped_column(Text, nullable=False)
    fecha_creacion: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    acronimo: Mapped[Optional[str]] = mapped_column(
        VARCHAR(20, charset="utf8mb4", collation="utf8mb4_spanish_ci"),
        comment="Nombre acortado",
    )
    ambito: Mapped[Optional[str]] = mapped_column(String(100))
    url: Mapped[Optional[str]] = mapped_column(Text)


class IUnidadExcelenciaLineaInvestigacion(Base):
    __tablename__ = "i_unidad_excelencia_linea_investigacion"

    idUdExcelencia: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class IUnidadExcelenciaPalabraClave(Base):
    __tablename__ = "i_unidad_excelencia_palabra_clave"

    idUdExcelencia: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    fecha: Mapped[Optional[datetime.date]] = mapped_column(Date)


class Institucion(Base):
    __tablename__ = "institucion"
    __table_args__ = (Index("id_ror", "id_ror", unique=True),)

    idInstitucion: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    id_ror: Mapped[Optional[str]] = mapped_column(String(9))
    tipo_organizacion: Mapped[Optional[str]] = mapped_column(
        VARCHAR(50, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    enlace: Mapped[Optional[str]] = mapped_column(
        VARCHAR(250, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    acronimos: Mapped[Optional[str]] = mapped_column(
        VARCHAR(100, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    coordenadas_lat: Mapped[Optional[str]] = mapped_column(
        "coordenadas.lat",
        VARCHAR(40, charset="utf8mb3", collation="utf8mb3_general_ci"),
    )
    coordenadas_lng: Mapped[Optional[str]] = mapped_column(
        "coordenadas.lng",
        VARCHAR(40, charset="utf8mb3", collation="utf8mb3_general_ci"),
    )
    ciudad: Mapped[Optional[str]] = mapped_column(
        VARCHAR(75, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    pais: Mapped[Optional[str]] = mapped_column(
        VARCHAR(75, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    fundref_preferido: Mapped[Optional[str]] = mapped_column(
        VARCHAR(75, charset="utf8mb3", collation="utf8mb3_general_ci")
    )


t_investigador_biblioteca = Table(
    "investigador_biblioteca",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("biblioteca", String(150)),
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
)


t_m_at = Table(
    "m_at",
    Base.metadata,
    Column("idFuente", INTEGER(9), nullable=False),
    Column("titulo", String(500)),
    Column("editorial", String(100), nullable=False),
    Column("tipo", String(100), nullable=False),
    Column("descuento", INTEGER(3)),
    Column("licencias_limitadas", TINYINT(1), server_default=text("0")),
    Column("promotor", String(100)),
    Column("agno", INTEGER(11)),
    Index("m_at_idFuente_IDX", "idFuente", "agno", unique=True),
)


class MCeaApq(Base):
    __tablename__ = "m_cea_apq"
    __table_args__ = (Index("monografia", "monografia"),)

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    idFuente: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    monografia: Mapped[int] = mapped_column(
        TINYINT(4), nullable=False, server_default=text("0")
    )
    coleccion: Mapped[Optional[str]] = mapped_column(String(200))
    universidad: Mapped[Optional[str]] = mapped_column(String(160))
    convocatoria: Mapped[Optional[str]] = mapped_column(String(22))
    agno: Mapped[Optional[str]] = mapped_column(String(4))
    internacionalidad: Mapped[Optional[int]] = mapped_column(TINYINT(1))
    fecha_expiracion: Mapped[Optional[datetime.date]] = mapped_column(Date)
    url: Mapped[Optional[str]] = mapped_column(
        VARCHAR(133, charset="utf8mb3", collation="utf8mb3_general_ci")
    )


class MCitescore(Base):
    __tablename__ = "m_citescore"
    __table_args__ = (
        Index("agno", "agno"),
        Index("idFuente", "idFuente"),
        Index("issn", "issn"),
        Index("issn_2", "issn", "agno", "categoria", unique=True),
        Index("revista", "revista", mysql_length={"revista": 255}),
        Index("revista_2", "revista", "agno", mysql_length={"revista": 255}),
    )

    id: Mapped[int] = mapped_column(
        BIGINT(20, unsigned=True), primary_key=True, autoincrement=True
    )
    revista: Mapped[str] = mapped_column(
        VARCHAR(500, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    issn: Mapped[str] = mapped_column(
        VARCHAR(9, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    agno: Mapped[str] = mapped_column(
        VARCHAR(4, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    categoria: Mapped[str] = mapped_column(
        VARCHAR(75, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    citeScore: Mapped[decimal.Decimal] = mapped_column(
        DECIMAL(6, 3), nullable=False, server_default=text("0.000")
    )
    posicion: Mapped[Optional[str]] = mapped_column(
        VARCHAR(15, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    cuartil: Mapped[Optional[str]] = mapped_column(
        VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    decil: Mapped[Optional[str]] = mapped_column(
        VARCHAR(3, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    tercil: Mapped[Optional[str]] = mapped_column(
        VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    idFuente: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MCsic(Base):
    __tablename__ = "m_csic"
    __table_args__ = (Index("editorial", "editorial"),)

    id: Mapped[int] = mapped_column(INTEGER(11), primary_key=True, autoincrement=True)
    editorial: Mapped[Optional[str]] = mapped_column(String(179))
    puntuacion: Mapped[Optional[str]] = mapped_column(String(5))


class MFecyt(Base):
    __tablename__ = "m_fecyt"
    __table_args__ = (
        Index("agno", "agno"),
        Index("eissn", "eissn"),
        Index("issn", "issn"),
        Index("issn_2", "issn", "eissn"),
        Index("titulo", "titulo", "convocatoria", "agno", "categoria", unique=True),
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    titulo: Mapped[Optional[str]] = mapped_column(String(160))
    issn: Mapped[Optional[str]] = mapped_column(String(11))
    eissn: Mapped[Optional[str]] = mapped_column(String(9))
    url: Mapped[Optional[str]] = mapped_column(String(133))
    convocatoria: Mapped[Optional[str]] = mapped_column(String(22))
    igualdad: Mapped[Optional[int]] = mapped_column(TINYINT(1))
    agno: Mapped[Optional[str]] = mapped_column(String(4))
    categoria: Mapped[Optional[str]] = mapped_column(String(52))
    puntuacion: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(4, 2))
    posicion: Mapped[Optional[str]] = mapped_column(String(5))
    cuartil: Mapped[Optional[str]] = mapped_column(String(2))


class MFecytNueva(Base):
    __tablename__ = "m_fecyt_nueva"
    __table_args__ = (
        Index("eissn", "eissn"),
        Index("issn", "issn"),
        Index("issn_2", "issn", "eissn"),
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    titulo: Mapped[Optional[str]] = mapped_column(String(160))
    issn: Mapped[Optional[str]] = mapped_column(String(11))
    eissn: Mapped[Optional[str]] = mapped_column(String(9))
    url: Mapped[Optional[str]] = mapped_column(String(133))
    convocatoria: Mapped[Optional[str]] = mapped_column(String(22))
    agno: Mapped[Optional[str]] = mapped_column(String(4))
    categoria: Mapped[Optional[str]] = mapped_column(String(52))
    puntuacion: Mapped[Optional[str]] = mapped_column(String(5))
    posicion: Mapped[Optional[str]] = mapped_column(String(5))
    cuartil: Mapped[Optional[str]] = mapped_column(String(2))


class MFecytOld(Base):
    __tablename__ = "m_fecyt_old"

    id: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    titulo: Mapped[str] = mapped_column(String(500), nullable=False)
    puntuacion: Mapped[decimal.Decimal] = mapped_column(DECIMAL(4, 2), nullable=False)
    cuartil: Mapped[str] = mapped_column(String(2), nullable=False)
    agno: Mapped[str] = mapped_column(String(4), nullable=False)
    categoria: Mapped[str] = mapped_column(String(100), nullable=False)
    issn: Mapped[Optional[str]] = mapped_column(String(15))
    eissn: Mapped[Optional[str]] = mapped_column(String(15))


class MIdr(Base):
    __tablename__ = "m_idr"
    __table_args__ = (
        Index("anualidad", "anualidad"),
        Index(
            "anualidad_categoria_idFuente",
            "anualidad",
            "categoria",
            "idFuente",
            unique=True,
        ),
        Index("categoria", "categoria"),
        Index("idFuente", "idFuente"),
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    prisma_id: Mapped[int] = mapped_column(BIGINT(20), nullable=False)
    totalRevista: Mapped[int] = mapped_column(INTEGER(11), nullable=False)
    titulo: Mapped[Optional[str]] = mapped_column(String(250))
    dialnet_id: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    issn: Mapped[Optional[str]] = mapped_column(String(9))
    anualidad: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    categoria: Mapped[Optional[str]] = mapped_column(String(50))
    factorImpacto: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(4, 3))
    cuartil: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    percentil: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    posicion: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    idFuente: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MIdr20230613(Base):
    __tablename__ = "m_idr_20230613"
    __table_args__ = (Index("anualidad", "anualidad"),)

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    prisma_id: Mapped[int] = mapped_column(BIGINT(20), nullable=False)
    totalRevista: Mapped[int] = mapped_column(INTEGER(11), nullable=False)
    titulo: Mapped[Optional[str]] = mapped_column(String(250))
    dialnet_id: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    issn: Mapped[Optional[str]] = mapped_column(String(9))
    anualidad: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    categoria: Mapped[Optional[str]] = mapped_column(String(50))
    factorImpacto: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(4, 3))
    cuartil: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    percentil: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    posicion: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MIdr20231114(Base):
    __tablename__ = "m_idr_20231114"
    __table_args__ = (
        Index("anualidad", "anualidad"),
        Index("idFuente", "idFuente"),
        Index("idFuente_2", "idFuente"),
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    prisma_id: Mapped[int] = mapped_column(BIGINT(20), nullable=False)
    totalRevista: Mapped[int] = mapped_column(INTEGER(11), nullable=False)
    titulo: Mapped[Optional[str]] = mapped_column(String(250))
    dialnet_id: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    issn: Mapped[Optional[str]] = mapped_column(String(9))
    anualidad: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    categoria: Mapped[Optional[str]] = mapped_column(String(50))
    factorImpacto: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(4, 3))
    cuartil: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    percentil: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    posicion: Mapped[Optional[int]] = mapped_column(INTEGER(11))
    idFuente: Mapped[Optional[int]] = mapped_column(INTEGER(11))


t_m_idr_20241030 = Table(
    "m_idr_20241030",
    Base.metadata,
    Column("id", BIGINT(20), nullable=False, server_default=text("0")),
    Column("titulo", VARCHAR(250, charset="utf8mb3", collation="utf8mb3_general_ci")),
    Column("dialnet_id", INTEGER(11)),
    Column("prisma_id", BIGINT(20), nullable=False),
    Column("issn", VARCHAR(9, charset="utf8mb3", collation="utf8mb3_general_ci")),
    Column("anualidad", INTEGER(11)),
    Column("categoria", VARCHAR(50, charset="utf8mb3", collation="utf8mb3_general_ci")),
    Column("factorImpacto", DECIMAL(4, 3)),
    Column("cuartil", INTEGER(11)),
    Column("percentil", INTEGER(11)),
    Column("posicion", INTEGER(11)),
    Column("totalRevista", INTEGER(11), nullable=False),
    Column("idFuente", INTEGER(11)),
)


class MInformes(Base):
    __tablename__ = "m_informes"
    __table_args__ = (
        Index("ambito", "ambito"),
        Index("ambito_2", "ambito", "tipo"),
        Index("basedatos", "basedatos", "tipo"),
        Index("identificador", "identificador"),
        Index("tipo", "tipo"),
    )

    idMetrica: Mapped[int] = mapped_column(
        INTEGER(11), primary_key=True, autoincrement=True
    )
    ambito: Mapped[str] = mapped_column(String(15), nullable=False)
    identificador: Mapped[str] = mapped_column(String(10), nullable=False)
    basedatos: Mapped[str] = mapped_column(String(10), nullable=False)
    tipo: Mapped[str] = mapped_column(String(15), nullable=False)
    valor: Mapped[str] = mapped_column(String(20), nullable=False)
    fechaActualizacion: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
    )
    identificadorInt: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MJci(Base):
    __tablename__ = "m_jci"
    __table_args__ = (
        Index("agno", "agno"),
        Index("agno_2", "agno", "categoria", "idFuente", unique=True),
        Index("idFuente", "idFuente"),
        Index("issn", "issn"),
    )

    id: Mapped[int] = mapped_column(
        BIGINT(20, unsigned=True), primary_key=True, autoincrement=True
    )
    revista: Mapped[str] = mapped_column(
        VARCHAR(500, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    agno: Mapped[str] = mapped_column(
        VARCHAR(4, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    categoria: Mapped[str] = mapped_column(
        VARCHAR(75, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    jci: Mapped[decimal.Decimal] = mapped_column(
        DECIMAL(6, 3), nullable=False, server_default=text("0.000")
    )
    percentil: Mapped[str] = mapped_column(
        VARCHAR(10, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    issn: Mapped[Optional[str]] = mapped_column(
        VARCHAR(9, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    issn_2: Mapped[Optional[str]] = mapped_column(String(9))
    posicion: Mapped[Optional[str]] = mapped_column(
        VARCHAR(15, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    cuartil: Mapped[Optional[str]] = mapped_column(
        VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    decil: Mapped[Optional[str]] = mapped_column(
        VARCHAR(3, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    tercil: Mapped[Optional[str]] = mapped_column(
        VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    idFuente: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MJcr(Base):
    __tablename__ = "m_jcr"
    __table_args__ = (
        Index("category", "category"),
        Index("decil", "decil"),
        Index("idFuente", "idFuente"),
        Index("impact_factor", "impact_factor"),
        Index("issn", "issn", "year"),
        Index("issn_2", "issn", "year", "edition"),
        Index("issn_2_2", "issn_2", "year"),
        Index("issn_2_3", "issn_2", "year", "edition"),
        Index("journal", "journal", mysql_length={"journal": 191}),
        Index("journal_2", "journal", "year", mysql_length={"journal": 191}),
        Index("journal_3", "journal", "issn", "issn_2", mysql_length={"journal": 191}),
        Index("quartile", "quartile"),
        Index("tercil", "tercil"),
        Index("year", "year"),
        Index("year_2", "year", "edition", "category", "idFuente", unique=True),
    )

    id_jcr: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    journal: Mapped[str] = mapped_column(String(500), nullable=False)
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    edition: Mapped[str] = mapped_column(String(100), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    issn: Mapped[Optional[str]] = mapped_column(String(9))
    issn_2: Mapped[Optional[str]] = mapped_column(String(9))
    impact_factor: Mapped[Optional[decimal.Decimal]] = mapped_column(
        DECIMAL(6, 3), server_default=text("0.000")
    )
    rank: Mapped[Optional[str]] = mapped_column(String(10))
    quartile: Mapped[Optional[str]] = mapped_column(String(10))
    decil: Mapped[Optional[str]] = mapped_column(String(8))
    tercil: Mapped[Optional[str]] = mapped_column(String(8))
    percentile: Mapped[Optional[str]] = mapped_column(String(10))
    idFuente: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MJcr20240117(Base):
    __tablename__ = "m_jcr_20240117"
    __table_args__ = (
        Index("category", "category"),
        Index("decil", "decil"),
        Index("idFuente", "idFuente"),
        Index("impact_factor", "impact_factor"),
        Index("issn", "issn", "year"),
        Index("issn_2", "issn", "year", "edition"),
        Index("issn_2_2", "issn_2", "year"),
        Index("issn_2_3", "issn_2", "year", "edition"),
        Index("journal", "journal", mysql_length={"journal": 191}),
        Index("journal_2", "journal", "year", mysql_length={"journal": 191}),
        Index("journal_3", "journal", "issn", "issn_2", mysql_length={"journal": 191}),
        Index("quartile", "quartile"),
        Index("tercil", "tercil"),
        Index("year", "year"),
    )

    id_jcr: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    journal: Mapped[str] = mapped_column(String(500), nullable=False)
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    edition: Mapped[str] = mapped_column(String(100), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    issn: Mapped[Optional[str]] = mapped_column(String(9))
    issn_2: Mapped[Optional[str]] = mapped_column(String(9))
    impact_factor: Mapped[Optional[decimal.Decimal]] = mapped_column(
        DECIMAL(6, 3), server_default=text("0.000")
    )
    rank: Mapped[Optional[str]] = mapped_column(String(10))
    quartile: Mapped[Optional[str]] = mapped_column(String(10))
    decil: Mapped[Optional[str]] = mapped_column(String(8))
    tercil: Mapped[Optional[str]] = mapped_column(String(8))
    idFuente: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MJcr20240205(Base):
    __tablename__ = "m_jcr_20240205"
    __table_args__ = (
        Index("category", "category"),
        Index("decil", "decil"),
        Index("idFuente", "idFuente"),
        Index("impact_factor", "impact_factor"),
        Index("issn", "issn", "year"),
        Index("issn_2", "issn", "year", "edition"),
        Index("issn_2_2", "issn_2", "year"),
        Index("issn_2_3", "issn_2", "year", "edition"),
        Index("journal", "journal", mysql_length={"journal": 191}),
        Index("journal_2", "journal", "year", mysql_length={"journal": 191}),
        Index("journal_3", "journal", "issn", "issn_2", mysql_length={"journal": 191}),
        Index("quartile", "quartile"),
        Index("tercil", "tercil"),
        Index("year", "year"),
    )

    id_jcr: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    journal: Mapped[str] = mapped_column(String(500), nullable=False)
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    edition: Mapped[str] = mapped_column(String(100), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    issn: Mapped[Optional[str]] = mapped_column(String(9))
    issn_2: Mapped[Optional[str]] = mapped_column(String(9))
    impact_factor: Mapped[Optional[decimal.Decimal]] = mapped_column(
        DECIMAL(6, 3), server_default=text("0.000")
    )
    rank: Mapped[Optional[str]] = mapped_column(String(10))
    quartile: Mapped[Optional[str]] = mapped_column(String(10))
    decil: Mapped[Optional[str]] = mapped_column(String(8))
    tercil: Mapped[Optional[str]] = mapped_column(String(8))
    idFuente: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MPublicaciones(Base):
    __tablename__ = "m_publicaciones"
    __table_args__ = (
        Index("idxPublicacion", "idPublicacion"),
        {"comment": "Métricas de las publicaciones"},
    )

    idMetrica: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    idPublicacion: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False
    )
    metrica: Mapped[str] = mapped_column(
        VARCHAR(25, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    basedatos: Mapped[str] = mapped_column(
        VARCHAR(10, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    valor: Mapped[str] = mapped_column(
        VARCHAR(25, charset="utf8mb3", collation="utf8mb3_spanish_ci"), nullable=False
    )
    fechaActualizacion: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
    )


t_m_scholar = Table(
    "m_scholar",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScholar", String(15), nullable=False),
    Column("nombre", String(100), nullable=False),
    Column("pag_us", TINYINT(1), nullable=False, server_default=text("0")),
    Column("citasTotal", INTEGER(10), nullable=False),
    Column("citasHace5", INTEGER(10), nullable=False),
    Column("indiceHTotal", INTEGER(10), nullable=False),
    Column("indiceHHace5", INTEGER(10), nullable=False),
    Column("indiceI10Total", INTEGER(10), nullable=False),
    Column("indiceI10Hace5", INTEGER(10), nullable=False),
    Column("nDocs", INTEGER(10), nullable=False),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


class MScholar20230515(Base):
    __tablename__ = "m_scholar_20230515"

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False
    )
    idScholar: Mapped[str] = mapped_column(String(15), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    pag_us: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    citasTotal: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    citasHace5: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    indiceHTotal: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    indiceHHace5: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    indiceI10Total: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    indiceI10Hace5: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    nDocs: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    fecha: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, primary_key=True, server_default=text("current_timestamp()")
    )
    eliminado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    comentario: Mapped[Optional[str]] = mapped_column(String(200))


t_m_scholar_20230613 = Table(
    "m_scholar_20230613",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScholar", String(15), nullable=False),
    Column("nombre", String(100), nullable=False),
    Column("pag_us", TINYINT(1), nullable=False, server_default=text("0")),
    Column("citasTotal", INTEGER(10), nullable=False),
    Column("citasHace5", INTEGER(10), nullable=False),
    Column("indiceHTotal", INTEGER(10), nullable=False),
    Column("indiceHHace5", INTEGER(10), nullable=False),
    Column("indiceI10Total", INTEGER(10), nullable=False),
    Column("indiceI10Hace5", INTEGER(10), nullable=False),
    Column("nDocs", INTEGER(10), nullable=False),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


t_m_scholar_20231113 = Table(
    "m_scholar_20231113",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScholar", String(15), nullable=False),
    Column("nombre", String(100), nullable=False),
    Column("pag_us", TINYINT(1), nullable=False, server_default=text("0")),
    Column("citasTotal", INTEGER(10), nullable=False),
    Column("citasHace5", INTEGER(10), nullable=False),
    Column("indiceHTotal", INTEGER(10), nullable=False),
    Column("indiceHHace5", INTEGER(10), nullable=False),
    Column("indiceI10Total", INTEGER(10), nullable=False),
    Column("indiceI10Hace5", INTEGER(10), nullable=False),
    Column("nDocs", INTEGER(10), nullable=False),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


t_m_scholar_20231218 = Table(
    "m_scholar_20231218",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScholar", String(15), nullable=False),
    Column("nombre", String(100), nullable=False),
    Column("pag_us", TINYINT(1), nullable=False, server_default=text("0")),
    Column("citasTotal", INTEGER(10), nullable=False),
    Column("citasHace5", INTEGER(10), nullable=False),
    Column("indiceHTotal", INTEGER(10), nullable=False),
    Column("indiceHHace5", INTEGER(10), nullable=False),
    Column("indiceI10Total", INTEGER(10), nullable=False),
    Column("indiceI10Hace5", INTEGER(10), nullable=False),
    Column("nDocs", INTEGER(10), nullable=False),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


t_m_scopus = Table(
    "m_scopus",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScopus", String(45), nullable=False),
    Column("citation_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("cited_by_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("coauthor_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("document_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("h_index", INTEGER(10), nullable=False, server_default=text("0")),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


t_m_scopus_20230918 = Table(
    "m_scopus_20230918",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScopus", String(45), nullable=False),
    Column("citation_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("cited_by_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("coauthor_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("document_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("h_index", INTEGER(10), nullable=False, server_default=text("0")),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


t_m_scopus_20231016 = Table(
    "m_scopus_20231016",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScopus", String(45), nullable=False),
    Column("citation_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("cited_by_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("coauthor_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("document_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("h_index", INTEGER(10), nullable=False, server_default=text("0")),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


t_m_scopus_20240214 = Table(
    "m_scopus_20240214",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScopus", String(45), nullable=False),
    Column("citation_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("cited_by_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("coauthor_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("document_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("h_index", INTEGER(10), nullable=False, server_default=text("0")),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


t_m_scopus_20240318 = Table(
    "m_scopus_20240318",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScopus", String(45), nullable=False),
    Column("citation_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("cited_by_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("coauthor_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("document_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("h_index", INTEGER(10), nullable=False, server_default=text("0")),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


t_m_scopus_AAAAMMDD = Table(
    "m_scopus_AAAAMMDD",
    Base.metadata,
    Column("idInvestigador", INTEGER(10, unsigned=True), nullable=False),
    Column("idScopus", String(45), nullable=False),
    Column("citation_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("cited_by_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("coauthor_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("document_count", INTEGER(10), nullable=False, server_default=text("0")),
    Column("h_index", INTEGER(10), nullable=False, server_default=text("0")),
    Column(
        "fecha", TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'")
    ),
    Column("comentario", String(200)),
    Column("eliminado", TINYINT(1), nullable=False, server_default=text("0")),
)


class MSjr(Base):
    __tablename__ = "m_sjr"
    __table_args__ = (
        Index("category", "category"),
        Index("decil", "decil"),
        Index("idFuente", "idFuente"),
        Index("impact_factor", "impact_factor"),
        Index("issn", "issn", "year"),
        Index("issn_2", "issn", "year", "category"),
        Index("issn_2_2", "issn_2", "year"),
        Index("issn_2_3", "issn_2", "year", "category"),
        Index("journal", "journal", mysql_length={"journal": 191}),
        Index("quartile", "quartile"),
        Index("tercil", "tercil"),
        Index("year", "year"),
        Index("year_2", "year", "idFuente"),
    )

    id_sjr: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    journal: Mapped[Optional[str]] = mapped_column(String(500))
    issn: Mapped[Optional[str]] = mapped_column(String(9))
    issn_2: Mapped[Optional[str]] = mapped_column(String(9))
    impact_factor: Mapped[Optional[decimal.Decimal]] = mapped_column(
        DECIMAL(6, 3), server_default=text("0.000")
    )
    rank: Mapped[Optional[str]] = mapped_column(String(10))
    quartile: Mapped[Optional[str]] = mapped_column(String(10))
    decil: Mapped[Optional[str]] = mapped_column(String(5))
    tercil: Mapped[Optional[str]] = mapped_column(String(5))
    idFuente: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MSjr20230510(Base):
    __tablename__ = "m_sjr_20230510"
    __table_args__ = (
        Index("category", "category"),
        Index("decil", "decil"),
        Index("impact_factor", "impact_factor"),
        Index("issn", "issn", "year"),
        Index("issn_2", "issn", "year", "category"),
        Index("issn_2_2", "issn_2", "year"),
        Index("issn_2_3", "issn_2", "year", "category"),
        Index("quartile", "quartile"),
        Index("tercil", "tercil"),
        Index("year", "year"),
    )

    id_sjr: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    journal: Mapped[Optional[str]] = mapped_column(String(500))
    issn: Mapped[Optional[str]] = mapped_column(String(9))
    issn_2: Mapped[Optional[str]] = mapped_column(String(9))
    impact_factor: Mapped[Optional[decimal.Decimal]] = mapped_column(
        DECIMAL(6, 3), server_default=text("0.000")
    )
    rank: Mapped[Optional[str]] = mapped_column(String(10))
    quartile: Mapped[Optional[str]] = mapped_column(String(10))
    decil: Mapped[Optional[str]] = mapped_column(String(5))
    tercil: Mapped[Optional[str]] = mapped_column(String(5))


class MSjr20250627(Base):
    __tablename__ = "m_sjr_20250627"
    __table_args__ = (
        Index("category", "category"),
        Index("decil", "decil"),
        Index("idFuente", "idFuente"),
        Index("impact_factor", "impact_factor"),
        Index("issn", "issn", "year"),
        Index("issn_2", "issn", "year", "category"),
        Index("issn_2_2", "issn_2", "year"),
        Index("issn_2_3", "issn_2", "year", "category"),
        Index("journal", "journal", mysql_length={"journal": 191}),
        Index("quartile", "quartile"),
        Index("tercil", "tercil"),
        Index("year", "year"),
        Index("year_2", "year", "idFuente"),
    )

    id_sjr: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    journal: Mapped[Optional[str]] = mapped_column(String(500))
    issn: Mapped[Optional[str]] = mapped_column(String(9))
    issn_2: Mapped[Optional[str]] = mapped_column(String(9))
    impact_factor: Mapped[Optional[decimal.Decimal]] = mapped_column(
        DECIMAL(6, 3), server_default=text("0.000")
    )
    rank: Mapped[Optional[str]] = mapped_column(String(10))
    quartile: Mapped[Optional[str]] = mapped_column(String(10))
    decil: Mapped[Optional[str]] = mapped_column(String(5))
    tercil: Mapped[Optional[str]] = mapped_column(String(5))
    idFuente: Mapped[Optional[int]] = mapped_column(INTEGER(11))


class MSpi(Base):
    __tablename__ = "m_spi"
    __table_args__ = (Index("agno", "agno"), Index("editorial", "editorial"))

    idMetrica: Mapped[int] = mapped_column(
        INTEGER(11), primary_key=True, autoincrement=True
    )
    editorial: Mapped[str] = mapped_column(
        VARCHAR(200, charset="utf8mb3", collation="utf8mb3_general_ci"), nullable=False
    )
    agno: Mapped[str] = mapped_column(
        VARCHAR(4, charset="utf8mb3", collation="utf8mb3_general_ci"),
        nullable=False,
        server_default=text("'2018'"),
    )
    categoria: Mapped[str] = mapped_column(
        VARCHAR(35, charset="utf8mb3", collation="utf8mb3_general_ci"), nullable=False
    )
    puntuacion: Mapped[float] = mapped_column(Float, nullable=False)
    ambito: Mapped[str] = mapped_column(
        VARCHAR(20, charset="utf8mb3", collation="utf8mb3_general_ci"), nullable=False
    )
    posicion: Mapped[int] = mapped_column(SMALLINT(6), nullable=False)
    total_ed: Mapped[int] = mapped_column(SMALLINT(6), nullable=False)
    cuartil: Mapped[str] = mapped_column(
        VARCHAR(2, charset="utf8mb4", collation="utf8mb4_unicode_ci"), nullable=False
    )
    editorial_original: Mapped[Optional[str]] = mapped_column(
        VARCHAR(200, charset="utf8mb3", collation="utf8mb3_general_ci")
    )


class MSpi20230522(Base):
    __tablename__ = "m_spi_20230522"
    __table_args__ = (Index("agno", "agno"), Index("editorial", "editorial"))

    idMetrica: Mapped[int] = mapped_column(
        INTEGER(11), primary_key=True, autoincrement=True
    )
    editorial: Mapped[str] = mapped_column(
        VARCHAR(200, charset="utf8mb3", collation="utf8mb3_general_ci"), nullable=False
    )
    agno: Mapped[str] = mapped_column(
        VARCHAR(4, charset="utf8mb3", collation="utf8mb3_general_ci"),
        nullable=False,
        server_default=text("'2018'"),
    )
    categoria: Mapped[str] = mapped_column(
        VARCHAR(35, charset="utf8mb3", collation="utf8mb3_general_ci"), nullable=False
    )
    puntuacion: Mapped[float] = mapped_column(Float, nullable=False)
    ambito: Mapped[str] = mapped_column(
        VARCHAR(20, charset="utf8mb3", collation="utf8mb3_general_ci"), nullable=False
    )
    posicion: Mapped[int] = mapped_column(SMALLINT(6), nullable=False)
    total_ed: Mapped[int] = mapped_column(SMALLINT(6), nullable=False)
    cuartil: Mapped[str] = mapped_column(
        VARCHAR(2, charset="utf8mb4", collation="utf8mb4_unicode_ci"), nullable=False
    )
    editorial_original: Mapped[Optional[str]] = mapped_column(
        VARCHAR(200, charset="utf8mb3", collation="utf8mb3_general_ci")
    )


class MSpi20230920(Base):
    __tablename__ = "m_spi_20230920"
    __table_args__ = (Index("agno", "agno"), Index("editorial", "editorial"))

    idMetrica: Mapped[int] = mapped_column(
        INTEGER(11), primary_key=True, autoincrement=True
    )
    editorial: Mapped[str] = mapped_column(
        VARCHAR(200, charset="utf8mb3", collation="utf8mb3_general_ci"), nullable=False
    )
    agno: Mapped[str] = mapped_column(
        VARCHAR(4, charset="utf8mb3", collation="utf8mb3_general_ci"),
        nullable=False,
        server_default=text("'2018'"),
    )
    categoria: Mapped[str] = mapped_column(
        VARCHAR(35, charset="utf8mb3", collation="utf8mb3_general_ci"), nullable=False
    )
    puntuacion: Mapped[float] = mapped_column(Float, nullable=False)
    ambito: Mapped[str] = mapped_column(
        VARCHAR(20, charset="utf8mb3", collation="utf8mb3_general_ci"), nullable=False
    )
    posicion: Mapped[int] = mapped_column(SMALLINT(6), nullable=False)
    total_ed: Mapped[int] = mapped_column(SMALLINT(6), nullable=False)
    cuartil: Mapped[str] = mapped_column(
        VARCHAR(2, charset="utf8mb4", collation="utf8mb4_unicode_ci"), nullable=False
    )
    editorial_original: Mapped[Optional[str]] = mapped_column(
        VARCHAR(200, charset="utf8mb3", collation="utf8mb3_general_ci")
    )


class PAccesoAbierto(Base):
    __tablename__ = "p_acceso_abierto"
    __table_args__ = (
        Index("pub_valor", "valor", "publicacion_id"),
        Index("publicacion_id", "publicacion_id"),
    )

    id: Mapped[int] = mapped_column(
        BIGINT(20, unsigned=True), primary_key=True, autoincrement=True
    )
    publicacion_id: Mapped[int] = mapped_column(
        BIGINT(20, unsigned=True), nullable=False
    )
    valor: Mapped[str] = mapped_column(String(50), nullable=False)
    origen: Mapped[str] = mapped_column(String(20), nullable=False)


class PAfiliacion(Base):
    __tablename__ = "p_afiliacion"
    __table_args__ = (
        Index("afiliacion", "afiliacion", mysql_length={"afiliacion": 191}),
        Index("afiliacion_2", "afiliacion", "pais", mysql_length={"afiliacion": 191}),
        Index("pais", "pais"),
        Index("scopus", "scopus_id"),
        {"comment": "Tabla con las afiliaciones de los autores"},
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    afiliacion: Mapped[str] = mapped_column(
        VARCHAR(200, charset="utf8mb4", collation="utf8mb4_spanish_ci"), nullable=False
    )
    pais: Mapped[str] = mapped_column(
        VARCHAR(50, charset="utf8mb4", collation="utf8mb4_spanish_ci"), nullable=False
    )
    scopus_id: Mapped[Optional[int]] = mapped_column(
        INTEGER(11), comment="Identificador de la afiliación en Scopus"
    )
    vease: Mapped[Optional[int]] = mapped_column(
        BIGINT(20), comment="Identificador de la afiliación normalizada"
    )
    nombre_ror: Mapped[Optional[str]] = mapped_column(
        VARCHAR(250, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    id_ror: Mapped[Optional[str]] = mapped_column(
        VARCHAR(15, charset="utf8mb3", collation="utf8mb3_general_ci")
    )


class PAutor(Base):
    __tablename__ = "p_autor"
    __table_args__ = (
        Index("idInvestigador", "idInvestigador"),
        Index("idPublicacion", "idPublicacion"),
        Index("idx_autor_rol_orden", "rol", "orden"),
        {"comment": "Autores de las publicaciones"},
    )

    idAutor: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    orden: Mapped[int] = mapped_column(
        SMALLINT(3), nullable=False, comment="Orden de la firma en la publicación"
    )
    firma: Mapped[str] = mapped_column(
        String(250), nullable=False, comment="Firma del autor en la publicación"
    )
    rol: Mapped[str] = mapped_column(
        String(20), nullable=False, comment="Rol en la publicación: autor, editor, etc."
    )
    contacto: Mapped[str] = mapped_column(
        String(1), nullable=False, server_default=text("'N'")
    )
    idPublicacion: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False
    )
    fechaActualizacion: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
    )
    idInvestigador: Mapped[Optional[int]] = mapped_column(
        INTEGER(10, unsigned=True),
        server_default=text("0"),
        comment="Identificador en la tabla 'investigador'. 0 si no es un autor US",
    )
    eliminado: Mapped[Optional[int]] = mapped_column(
        TINYINT(1), server_default=text("0")
    )


class PAutorAfiliacion(Base):
    __tablename__ = "p_autor_afiliacion"
    __table_args__ = {"comment": "Guarda la relación entre un autor y sus afiliaciones"}

    autor_id: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, comment="Identificador de la tabla p_autor"
    )
    afiliacion_id: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, comment="Identificador de la tabla p_afiliacion"
    )


class PDatoFuente(Base):
    __tablename__ = "p_dato_fuente"
    __table_args__ = (
        Index("fuente1_idx", "idFuente"),
        Index("tipo", "tipo"),
        Index("valor", "valor"),
        {"comment": "Datos de las fuentes de las publicaciones"},
    )

    idIdentificador: Mapped[int] = mapped_column(
        BIGINT(20, unsigned=True), primary_key=True, autoincrement=True
    )
    idFuente: Mapped[int] = mapped_column(BIGINT(10, unsigned=True), nullable=False)
    tipo: Mapped[str] = mapped_column(String(10), nullable=False)
    valor: Mapped[str] = mapped_column(String(150), nullable=False)
    actualizado: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
    )
    comentario: Mapped[Optional[str]] = mapped_column(String(500))


class PDatoPublicacion(Base):
    __tablename__ = "p_dato_publicacion"
    __table_args__ = (
        Index("fk_p_dato_publicacion_p_publicacion1_idx", "idPublicacion"),
        Index("tipo", "tipo"),
        Index("valor", "valor", mysql_length={"valor": 191}),
    )

    idDato: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(String(100), nullable=False)
    valor: Mapped[str] = mapped_column(String(250), nullable=False)
    idPublicacion: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False
    )
    origen: Mapped[Optional[str]] = mapped_column(String(100))


class PEditor(Base):
    __tablename__ = "p_editor"
    __table_args__ = (
        Index("nombre", "nombre", mysql_length={"nombre": 191}),
        Index("pais", "pais"),
        Index("tipo", "tipo"),
        {"comment": "Editoriales de las fuentes"},
    )

    id: Mapped[int] = mapped_column(
        BIGINT(20, unsigned=True), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    tipo: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("'Otros'")
    )
    pais: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("'Desconocido'")
    )
    visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("1")
    )
    vease: Mapped[Optional[int]] = mapped_column(
        BIGINT(20), comment="Identificador de la editorial/editor normalizada"
    )
    url: Mapped[Optional[str]] = mapped_column(
        String(260), comment="URL de la editorial"
    )


class PEditorMal(Base):
    __tablename__ = "p_editor_mal"
    __table_args__ = (
        Index("nombre", "nombre", mysql_length={"nombre": 191}),
        Index("pais", "pais"),
        Index("tipo", "tipo"),
        {"comment": "Editoriales de las fuentes"},
    )

    id: Mapped[int] = mapped_column(
        BIGINT(20, unsigned=True), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    tipo: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("'Otros'")
    )
    pais: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("'Desconocido'")
    )
    visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("1")
    )
    vease: Mapped[Optional[int]] = mapped_column(
        BIGINT(20), comment="Identificador de la editorial/editor normalizada"
    )
    url: Mapped[Optional[str]] = mapped_column(
        String(260), comment="URL de la editorial"
    )


t_p_fecha_publicacion = Table(
    "p_fecha_publicacion",
    Base.metadata,
    Column("idPublicacion", INTEGER(11), nullable=False),
    Column("tipo", String(100), nullable=False),
    Column("mes", INTEGER(11)),
    Column("agno", INTEGER(11), nullable=False),
    Column("dia", INTEGER(11)),
    Index("p_fecha_publicacion_idPublicacion_IDX", "idPublicacion", "tipo"),
)


class PFinanciacion(Base):
    __tablename__ = "p_financiacion"
    __table_args__ = (Index("codigo", "codigo"),)

    idFinanciacion: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    publicacion_id: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False
    )
    codigo: Mapped[Optional[str]] = mapped_column(String(50))
    agencia: Mapped[Optional[str]] = mapped_column(String(300))
    idProyecto: Mapped[Optional[int]] = mapped_column(INTEGER(15))


class PFuente(Base):
    __tablename__ = "p_fuente"
    __table_args__ = (
        Index("tipo", "tipo"),
        Index("titulo", "titulo", mysql_length={"titulo": 191}),
        {"comment": "Fuente de la publicación"},
    )

    idFuente: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(
        String(25), nullable=False, comment="revista, libro,..."
    )
    titulo: Mapped[str] = mapped_column(String(800), nullable=False)
    origen: Mapped[str] = mapped_column(String(50), nullable=False)
    eliminado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    editorial: Mapped[Optional[str]] = mapped_column(String(200))
    validado: Mapped[Optional[int]] = mapped_column(
        TINYINT(1), server_default=text("1")
    )
    fechaActualizacion: Mapped[Optional[datetime.datetime]] = mapped_column(
        TIMESTAMP,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
    )


class PIdentificadorFuente(Base):
    __tablename__ = "p_identificador_fuente"
    __table_args__ = (
        Index("fuente1_idx", "idFuente"),
        Index("tipo", "tipo"),
        Index("valor", "valor"),
        {"comment": "Identificadores de las fuentes de las publicaciones"},
    )

    idIdentificador: Mapped[int] = mapped_column(
        BIGINT(20, unsigned=True), primary_key=True, autoincrement=True
    )
    idFuente: Mapped[int] = mapped_column(BIGINT(10, unsigned=True), nullable=False)
    tipo: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="eissn, eisbn, isbn, issn, doi, wos, etc"
    )
    valor: Mapped[str] = mapped_column(String(50), nullable=False)
    eliminado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    actualizado: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
    )
    origen: Mapped[Optional[str]] = mapped_column(String(20))
    comentario: Mapped[Optional[str]] = mapped_column(String(500))


class PIdentificadorPublicacion(Base):
    __tablename__ = "p_identificador_publicacion"
    __table_args__ = (
        Index("fk_p_identificador_publicacion_p_publicacion1_idx", "idPublicacion"),
        Index("tipo", "tipo"),
        Index("tipo_2", "tipo", "valor", unique=True),
        Index("valor", "valor"),
        {"comment": "Identificadores de las publicaciones"},
    )

    idIdentificador: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="doi,scopus, wos, idus"
    )
    valor: Mapped[str] = mapped_column(String(100), nullable=False)
    eliminado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    idPublicacion: Mapped[Optional[int]] = mapped_column(INTEGER(10, unsigned=True))
    origen: Mapped[Optional[str]] = mapped_column(String(20))
    comentario: Mapped[Optional[str]] = mapped_column(String(500))


class PPublicacion(Base):
    __tablename__ = "p_publicacion"
    __table_args__ = (
        Index("agno", "agno"),
        Index("fuente1_idx", "idFuente"),
        Index("idPublicacion", "idPublicacion", "agno", "validado", "eliminado"),
        Index("tipo", "tipo"),
        Index("titulo", "titulo", mysql_length={"titulo": 191}),
    )

    idPublicacion: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Tipo de publicación: artículo, nota, revisión, ponencia",
    )
    titulo: Mapped[str] = mapped_column(String(1000), nullable=False)
    agno: Mapped[str] = mapped_column(String(4), nullable=False)
    idFuente: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False, server_default=text("0")
    )
    origen: Mapped[str] = mapped_column(String(50), nullable=False)
    validado: Mapped[int] = mapped_column(
        TINYINT(1),
        nullable=False,
        server_default=text("1"),
        comment="Indica si la publicación ha sido validada",
    )
    fechaActualizacion: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
    )
    eliminado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )


class PPublicacion20260410(Base):
    __tablename__ = "p_publicacion_20260410"
    __table_args__ = (
        Index("agno", "agno"),
        Index("fuente1_idx", "idFuente"),
        Index("idPublicacion", "idPublicacion", "agno", "validado", "eliminado"),
        Index("tipo", "tipo"),
        Index("titulo", "titulo", mysql_length={"titulo": 191}),
    )

    idPublicacion: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Tipo de publicación: artículo, nota, revisión, ponencia",
    )
    titulo: Mapped[str] = mapped_column(String(1000), nullable=False)
    agno: Mapped[str] = mapped_column(String(4), nullable=False)
    idFuente: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False, server_default=text("0")
    )
    origen: Mapped[str] = mapped_column(String(50), nullable=False)
    validado: Mapped[int] = mapped_column(
        TINYINT(1),
        nullable=False,
        server_default=text("1"),
        comment="Indica si la publicación ha sido validada",
    )
    fechaActualizacion: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
    )
    eliminado: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )


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
)


class RamasPrueba(Base):
    __tablename__ = "ramas_prueba"
    __table_args__ = (
        Index("Departamento", "Departamento"),
        Index("Rama", "Rama"),
        Index("Área", "Área"),
    )

    Id: Mapped[int] = mapped_column(
        INTEGER(4), primary_key=True, server_default=text("0")
    )
    Departamento: Mapped[Optional[str]] = mapped_column(String(99))
    Área: Mapped[Optional[str]] = mapped_column(String(58))
    Rama: Mapped[Optional[str]] = mapped_column(String(29))


t_ranking_scholar = Table(
    "ranking_scholar",
    Base.metadata,
    Column("Id", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("idScholar", String(100)),
    Column("nombre", String(226)),
    Column("idDepartamento", String(4)),
    Column("idArea", SMALLINT(3, unsigned=True, zerofill=True)),
    Column("idRamaAneca", TINYINT(2, unsigned=True), server_default=text("'0'")),
    Column("idRama", TINYINT(2, unsigned=True), server_default=text("'0'")),
    Column("nDocs", INTEGER(10)),
    Column("citasTotal", INTEGER(10)),
    Column("indiceHTotal", INTEGER(10)),
    Column("indiceI10Total", INTEGER(10)),
    Column("citasHace5", INTEGER(10)),
    Column("indiceHHace5", INTEGER(10)),
    Column("indiceI10Hace5", INTEGER(10)),
)


t_ranking_scholar_mejorado = Table(
    "ranking_scholar_mejorado",
    Base.metadata,
    Column("Id", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("idScholar", String(100)),
    Column("nombre", String(226)),
    Column("idDepartamento", String(4)),
    Column("idArea", SMALLINT(3, unsigned=True, zerofill=True)),
    Column("idRamaAneca", TINYINT(2, unsigned=True), server_default=text("'0'")),
    Column("idRama", DECIMAL(3, 0), server_default=text("'0'")),
    Column("nDocs", INTEGER(10)),
    Column("citasTotal", INTEGER(10)),
    Column("indiceHTotal", INTEGER(10)),
    Column("indiceI10Total", INTEGER(10)),
    Column("citasHace5", INTEGER(10)),
    Column("indiceHHace5", INTEGER(10)),
    Column("indiceI10Hace5", INTEGER(10)),
)


t_ranking_scopus = Table(
    "ranking_scopus",
    Base.metadata,
    Column("idInves", INTEGER(10, unsigned=True), server_default=text("'0'")),
    Column("idScopus", String(100)),
    Column("nombre", String(226)),
    Column("idDepartamento", String(4)),
    Column("idArea", SMALLINT(3, unsigned=True, zerofill=True)),
    Column("idRama", DECIMAL(3, 0), server_default=text("'0'")),
    Column("idRamaAneca", TINYINT(2, unsigned=True), server_default=text("'0'")),
    Column("citation_count", INTEGER(10), server_default=text("'0'")),
    Column("cited_by_count", INTEGER(10), server_default=text("'0'")),
    Column("coauthor_count", INTEGER(10), server_default=text("'0'")),
    Column("document_count", INTEGER(10), server_default=text("'0'")),
    Column("h_index", INTEGER(10), server_default=text("'0'")),
)


t_re = Table(
    "re",
    Base.metadata,
    Column("id", BIGINT(20, unsigned=True), nullable=False, server_default=text("0")),
    Column(
        "revista",
        VARCHAR(500, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
        nullable=False,
    ),
    Column(
        "issn",
        VARCHAR(9, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
        nullable=False,
    ),
    Column(
        "agno",
        VARCHAR(4, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
        nullable=False,
    ),
    Column(
        "categoria",
        VARCHAR(75, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
        nullable=False,
    ),
    Column("citeScore", DECIMAL(6, 3), nullable=False, server_default=text("0.000")),
    Column("posicion", VARCHAR(15, charset="utf8mb3", collation="utf8mb3_spanish_ci")),
    Column("cuartil", VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")),
    Column("decil", VARCHAR(3, charset="utf8mb3", collation="utf8mb3_spanish_ci")),
    Column("tercil", VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")),
)


t_tesis_fecha_lectura_mal = Table(
    "tesis_fecha_lectura_mal",
    Base.metadata,
    Column("URL", String(56)),
    Column("fecha_fectura", String(250)),
)
