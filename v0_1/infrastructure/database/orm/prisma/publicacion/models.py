import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    SmallInteger,
    String,
    Table,
    func,
    text,
)
from sqlalchemy.dialects.mysql import TIMESTAMP, TINYINT, VARCHAR
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.orm.base import Base, get_table_args

SCHEMA_NAME = "prisma"


# ---------------------------------------------------------------------------
# Declarative ORM Models
# ---------------------------------------------------------------------------


class PAccesoAbiertoORM(Base):
    __tablename__ = "p_acceso_abierto"
    __table_args__ = get_table_args(
        Index("idx_paa_val_pub", "valor", "publicacion_id"),
        Index("idx_paa_publicacion_id", "publicacion_id"),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    publicacion_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    valor: Mapped[str] = mapped_column(String(50), nullable=False)
    origen: Mapped[str] = mapped_column(String(20), nullable=False)


class PAfiliacionORM(Base):
    __tablename__ = "p_afiliacion"
    __table_args__ = get_table_args(
        Index("idx_paf_afiliacion", "afiliacion"),
        Index("idx_paf_afiliacion_pais", "afiliacion", "pais"),
        Index("idx_paf_pais", "pais"),
        Index("idx_paf_scopus", "scopus_id"),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    afiliacion: Mapped[str] = mapped_column(String(200), nullable=False)
    pais: Mapped[str] = mapped_column(String(50), nullable=False)
    scopus_id: Mapped[int | None] = mapped_column(
        Integer, comment="Identificador de la afiliación en Scopus"
    )
    vease: Mapped[int | None] = mapped_column(
        BigInteger, comment="Identificador de la afiliación normalizada"
    )
    nombre_ror: Mapped[str | None] = mapped_column(String(250))
    id_ror: Mapped[str | None] = mapped_column(String(15))


class PAutorORM(Base):
    __tablename__ = "p_autor"
    __table_args__ = get_table_args(
        Index("idx_pau_idInvestigador", "idInvestigador"),
        Index("idx_pau_idPublicacion", "idPublicacion"),
        Index("idx_pau_rol_orden", "rol", "orden"),
        schema_name=SCHEMA_NAME,
    )

    idAutor: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    orden: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, comment="Orden de la firma en la publicación"
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
    idPublicacion: Mapped[int] = mapped_column(Integer, nullable=False)
    fechaActualizacion: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    idInvestigador: Mapped[int | None] = mapped_column(
        Integer,
        server_default=text("0"),
        comment="Identificador en la tabla 'investigador'. 0 si no es un autor US",
    )
    eliminado: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        server_default=text("0"),
    )


class PAutorAfiliacionORM(Base):
    __tablename__ = "p_autor_afiliacion"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    autor_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, comment="Identificador de la tabla p_autor"
    )
    afiliacion_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, comment="Identificador de la tabla p_afiliacion"
    )


class PDatoFuenteORM(Base):
    __tablename__ = "p_dato_fuente"
    __table_args__ = get_table_args(
        Index("idx_pdf_fuente", "idFuente"),
        Index("idx_pdf_tipo", "tipo"),
        Index("idx_pdf_valor", "valor"),
        schema_name=SCHEMA_NAME,
    )

    idIdentificador: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    idFuente: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tipo: Mapped[str] = mapped_column(String(10), nullable=False)
    valor: Mapped[str] = mapped_column(String(150), nullable=False)
    actualizado: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    comentario: Mapped[str | None] = mapped_column(String(500))


class PDatoPublicacionORM(Base):
    __tablename__ = "p_dato_publicacion"
    __table_args__ = get_table_args(
        Index("idx_pdp_idPublicacion", "idPublicacion"),
        Index("idx_pdp_tipo", "tipo"),
        Index("idx_pdp_valor", "valor"),
        schema_name=SCHEMA_NAME,
    )

    idDato: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tipo: Mapped[str] = mapped_column(String(100), nullable=False)
    valor: Mapped[str] = mapped_column(
        String(300).with_variant(
            VARCHAR(300, charset="utf8mb3", collation="utf8mb3_spanish_ci"),
            "mysql",
            "mariadb",
        ),
        nullable=False,
    )
    idPublicacion: Mapped[int] = mapped_column(Integer, nullable=False)


class PEditorORM(Base):
    __tablename__ = "p_editor"
    __table_args__ = get_table_args(
        Index("idx_ped_nombre", "nombre"),
        Index("idx_ped_pais", "pais"),
        Index("idx_ped_tipo", "tipo"),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    tipo: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("'Otros'")
    )
    pais: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("'Desconocido'")
    )
    visible: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("1"),
    )
    vease: Mapped[int | None] = mapped_column(
        BigInteger, comment="Identificador de la editorial/editor normalizada"
    )
    url: Mapped[str | None] = mapped_column(String(260), comment="URL de la editorial")


class PFinanciacionORM(Base):
    __tablename__ = "p_financiacion"
    __table_args__ = get_table_args(
        Index("idx_pfi_codigo", "codigo"), schema_name=SCHEMA_NAME
    )

    idFinanciacion: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    publicacion_id: Mapped[int] = mapped_column(Integer, nullable=False)
    codigo: Mapped[str | None] = mapped_column(String(50))
    agencia: Mapped[str | None] = mapped_column(String(300))
    idProyecto: Mapped[int | None] = mapped_column(Integer)
    eliminado: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        server_default=text("0"),
    )


class PFuenteORM(Base):
    __tablename__ = "p_fuente"
    __table_args__ = get_table_args(
        Index("idx_pfu_tipo", "tipo"),
        Index("idx_pfu_titulo", "titulo"),
        schema_name=SCHEMA_NAME,
    )

    idFuente: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tipo: Mapped[str] = mapped_column(
        String(25), nullable=False, comment="revista, libro,..."
    )
    titulo: Mapped[str] = mapped_column(String(800), nullable=False)
    origen: Mapped[str] = mapped_column(String(50), nullable=False)
    eliminado: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    )
    editorial: Mapped[str | None] = mapped_column(String(200))
    validado: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        server_default=text("1"),
    )
    fechaActualizacion: Mapped[datetime.datetime | None] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        server_default=func.now(),
        onupdate=func.now(),
    )


class PIdentificadorFuenteORM(Base):
    __tablename__ = "p_identificador_fuente"
    __table_args__ = get_table_args(
        Index("idx_pif_fuente", "idFuente"),
        Index("idx_pif_tipo", "tipo"),
        Index("idx_pif_valor", "valor"),
        schema_name=SCHEMA_NAME,
    )

    idIdentificador: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    idFuente: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tipo: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="eissn, eisbn, isbn, issn, doi, wos, etc"
    )
    valor: Mapped[str] = mapped_column(String(50), nullable=False)
    eliminado: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    )
    actualizado: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    origen: Mapped[str | None] = mapped_column(String(20))
    comentario: Mapped[str | None] = mapped_column(String(500))


class PIdentificadorPublicacionORM(Base):
    __tablename__ = "p_identificador_publicacion"
    __table_args__ = get_table_args(
        Index("idx_pip_idPublicacion", "idPublicacion"),
        Index("idx_pip_tipo", "tipo"),
        Index("idx_pip_tipo_valor_unique", "tipo", "valor", unique=True),
        Index("idx_pip_valor", "valor"),
        schema_name=SCHEMA_NAME,
    )

    idIdentificador: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="doi,scopus, wos, idus"
    )
    valor: Mapped[str] = mapped_column(String(100), nullable=False)
    eliminado: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    )
    idPublicacion: Mapped[int | None] = mapped_column(Integer)
    origen: Mapped[str | None] = mapped_column(String(20))
    comentario: Mapped[str | None] = mapped_column(String(500))


class PPublicacionORM(Base):
    __tablename__ = "p_publicacion"
    __table_args__ = get_table_args(
        Index("idx_ppu_agno", "agno"),
        Index("idx_ppu_fuente", "idFuente"),
        Index(
            "idx_ppu_combo_state",
            "idPublicacion",
            "agno",
            "validado",
            "eliminado",
        ),
        Index("idx_ppu_tipo", "tipo"),
        Index("idx_ppu_titulo", "titulo"),
        schema_name=SCHEMA_NAME,
    )

    idPublicacion: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    tipo: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Tipo de publicación: artículo, nota, revisión, ponencia",
    )
    titulo: Mapped[str] = mapped_column(String(1000), nullable=False)
    agno: Mapped[str] = mapped_column(String(4), nullable=False)
    idFuente: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    origen: Mapped[str] = mapped_column(String(50), nullable=False)
    validado: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("1"),
        comment="Indica si la publicación ha sido validada",
    )
    fechaActualizacion: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    eliminado: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    )


# ---------------------------------------------------------------------------
# Reflection Tables / Views
# ---------------------------------------------------------------------------

t_p_fecha_publicacion = Table(
    "p_fecha_publicacion",
    Base.metadata,
    Column("idPublicacion", Integer, nullable=False),
    Column("tipo", String(100), nullable=False),
    Column("mes", Integer),
    Column("agno", Integer, nullable=False),
    Column("dia", Integer),
    Index("idx_pfp_idPublicacion_tipo", "idPublicacion", "tipo"),
    schema=SCHEMA_NAME,
)
