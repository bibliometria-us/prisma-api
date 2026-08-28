import datetime

from sqlalchemy import (
    TIMESTAMP,
    Column,
    Index,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.mysql import (
    BIGINT,
    INTEGER,
    SMALLINT,
    TINYINT,
    VARCHAR,
)
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.models.base import Base


class PAccesoAbierto(Base):
    __tablename__ = "p_acceso_abierto"
    __table_args__ = (
        Index("pub_valor", "valor", "publicacion_id"),
        Index("publicacion_id", "publicacion_id"),
        {"schema": "prisma"},
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
        {"schema": "prisma"},
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    afiliacion: Mapped[str] = mapped_column(
        VARCHAR(200, charset="utf8mb4", collation="utf8mb4_spanish_ci"), nullable=False
    )
    pais: Mapped[str] = mapped_column(
        VARCHAR(50, charset="utf8mb4", collation="utf8mb4_spanish_ci"), nullable=False
    )
    scopus_id: Mapped[int | None] = mapped_column(
        INTEGER(11), comment="Identificador de la afiliación en Scopus"
    )
    vease: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Identificador de la afiliación normalizada"
    )
    nombre_ror: Mapped[str | None] = mapped_column(
        VARCHAR(250, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    id_ror: Mapped[str | None] = mapped_column(
        VARCHAR(15, charset="utf8mb3", collation="utf8mb3_general_ci")
    )


class PAutor(Base):
    __tablename__ = "p_autor"
    __table_args__ = (
        Index("idInvestigador", "idInvestigador"),
        Index("idPublicacion", "idPublicacion"),
        Index("idx_autor_rol_orden", "rol", "orden"),
        {"comment": "Autores de las publicaciones"},
        {"schema": "prisma"},
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
    idInvestigador: Mapped[int | None] = mapped_column(
        INTEGER(10, unsigned=True),
        server_default=text("0"),
        comment="Identificador en la tabla 'investigador'. 0 si no es un autor US",
    )
    eliminado: Mapped[int | None] = mapped_column(TINYINT(1), server_default=text("0"))


class PAutorAfiliacion(Base):
    __tablename__ = "p_autor_afiliacion"
    __table_args__ = {
        "comment": "Guarda la relación entre un autor y sus afiliaciones",
        "schema": "prisma",
    }

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
        {"schema": "prisma"},
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
    comentario: Mapped[str | None] = mapped_column(String(500))


class PDatoPublicacion(Base):
    __tablename__ = "p_dato_publicacion"
    __table_args__ = (
        Index("fk_p_dato_publicacion_p_publicacion1_idx", "idPublicacion"),
        Index("tipo", "tipo"),
        Index("valor", "valor", mysql_length={"valor": 191}),
        {"schema": "prisma"},
    )

    idDato: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(String(100), nullable=False)
    valor: Mapped[str] = mapped_column(String(250), nullable=False)
    idPublicacion: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False
    )
    origen: Mapped[str | None] = mapped_column(String(100))


class PEditor(Base):
    __tablename__ = "p_editor"
    __table_args__ = (
        Index("nombre", "nombre", mysql_length={"nombre": 191}),
        Index("pais", "pais"),
        Index("tipo", "tipo"),
        {"comment": "Editoriales de las fuentes"},
        {"schema": "prisma"},
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
    vease: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Identificador de la editorial/editor normalizada"
    )
    url: Mapped[str | None] = mapped_column(String(260), comment="URL de la editorial")


t_p_fecha_publicacion = Table(
    "p_fecha_publicacion",
    Base.metadata,
    Column("idPublicacion", INTEGER(11), nullable=False),
    Column("tipo", String(100), nullable=False),
    Column("mes", INTEGER(11)),
    Column("agno", INTEGER(11), nullable=False),
    Column("dia", INTEGER(11)),
    Index("p_fecha_publicacion_idPublicacion_IDX", "idPublicacion", "tipo"),
    schema="prisma",
)


class PFinanciacion(Base):
    __tablename__ = "p_financiacion"
    __table_args__ = (
        Index("codigo", "codigo"),
        {"schema": "prisma"},
    )

    idFinanciacion: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    publicacion_id: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), nullable=False
    )
    codigo: Mapped[str | None] = mapped_column(String(50))
    agencia: Mapped[str | None] = mapped_column(String(300))
    idProyecto: Mapped[int | None] = mapped_column(INTEGER(15))


class PFuente(Base):
    __tablename__ = "p_fuente"
    __table_args__ = (
        Index("tipo", "tipo"),
        Index("titulo", "titulo", mysql_length={"titulo": 191}),
        {"comment": "Fuente de la publicación"},
        {"schema": "prisma"},
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
    editorial: Mapped[str | None] = mapped_column(String(200))
    validado: Mapped[int | None] = mapped_column(TINYINT(1), server_default=text("1"))
    fechaActualizacion: Mapped[datetime.datetime | None] = mapped_column(
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
        {"schema": "prisma"},
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
    origen: Mapped[str | None] = mapped_column(String(20))
    comentario: Mapped[str | None] = mapped_column(String(500))


class PIdentificadorPublicacion(Base):
    __tablename__ = "p_identificador_publicacion"
    __table_args__ = (
        Index("fk_p_identificador_publicacion_p_publicacion1_idx", "idPublicacion"),
        Index("tipo", "tipo"),
        Index("tipo_2", "tipo", "valor", unique=True),
        Index("valor", "valor"),
        {"comment": "Identificadores de las publicaciones"},
        {"schema": "prisma"},
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
    idPublicacion: Mapped[int | None] = mapped_column(INTEGER(10, unsigned=True))
    origen: Mapped[str | None] = mapped_column(String(20))
    comentario: Mapped[str | None] = mapped_column(String(500))


class PPublicacion(Base):
    __tablename__ = "p_publicacion"
    __table_args__ = (
        Index("agno", "agno"),
        Index("fuente1_idx", "idFuente"),
        Index("idPublicacion", "idPublicacion", "agno", "validado", "eliminado"),
        Index("tipo", "tipo"),
        Index("titulo", "titulo", mysql_length={"titulo": 191}),
        {"schema": "prisma"},
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
