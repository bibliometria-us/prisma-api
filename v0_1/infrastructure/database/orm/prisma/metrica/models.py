import datetime
import decimal

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Table,
    func,
    text,
)
from sqlalchemy.dialects.mysql import TIMESTAMP, TINYINT
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.orm.base import Base, get_table_args

SCHEMA_NAME = "prisma"


# ---------------------------------------------------------------------------
# Reflection Tables / Views
# ---------------------------------------------------------------------------

t_m_at = Table(
    "m_at",
    Base.metadata,
    Column("idFuente", Integer, nullable=False),
    Column("titulo", String(500)),
    Column("editorial", String(100), nullable=False),
    Column("tipo", String(100), nullable=False),
    Column("descuento", Integer),
    Column(
        "licencias_limitadas",
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        server_default=text("0"),
    ),
    Column("promotor", String(100)),
    Column("agno", Integer),
    Index("m_at_idFuente_IDX", "idFuente", "agno", unique=True),
    schema=SCHEMA_NAME,
)

t_m_scholar = Table(
    "m_scholar",
    Base.metadata,
    Column("idInvestigador", Integer, nullable=False),
    Column("idScholar", String(15), nullable=False),
    Column("nombre", String(100), nullable=False),
    Column(
        "pag_us",
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    ),
    Column("citasTotal", Integer, nullable=False),
    Column("citasHace5", Integer, nullable=False),
    Column("indiceHTotal", Integer, nullable=False),
    Column("indiceHHace5", Integer, nullable=False),
    Column("indiceI10Total", Integer, nullable=False),
    Column("indiceI10Hace5", Integer, nullable=False),
    Column("nDocs", Integer, nullable=False),
    Column(
        "fecha",
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
    ),
    Column("comentario", String(200)),
    Column(
        "eliminado",
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    ),
    schema=SCHEMA_NAME,
)

t_m_scopus = Table(
    "m_scopus",
    Base.metadata,
    Column("idInvestigador", Integer, nullable=False),
    Column("idScopus", String(45), nullable=False),
    Column("citation_count", Integer, nullable=False, server_default=text("0")),
    Column("cited_by_count", Integer, nullable=False, server_default=text("0")),
    Column("coauthor_count", Integer, nullable=False, server_default=text("0")),
    Column("document_count", Integer, nullable=False, server_default=text("0")),
    Column("h_index", Integer, nullable=False, server_default=text("0")),
    Column(
        "fecha",
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
    ),
    Column("comentario", String(200)),
    Column(
        "eliminado",
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    ),
    schema=SCHEMA_NAME,
)


# ---------------------------------------------------------------------------
# Declarative ORM Models
# ---------------------------------------------------------------------------


class MCeaApqORM(Base):
    __tablename__ = "m_cea_apq"
    __table_args__ = get_table_args(
        Index("idx_cea_monografia", "monografia"), schema_name=SCHEMA_NAME
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    idFuente: Mapped[int] = mapped_column(Integer, nullable=False)
    monografia: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(4), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    )
    coleccion: Mapped[str | None] = mapped_column(String(200))
    universidad: Mapped[str | None] = mapped_column(String(160))
    convocatoria: Mapped[str | None] = mapped_column(String(22))
    agno: Mapped[str | None] = mapped_column(String(4))
    internacionalidad: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb")
    )
    fecha_expiracion: Mapped[datetime.date | None] = mapped_column(Date)
    url: Mapped[str | None] = mapped_column(String(133))


class MCitescoreORM(Base):
    __tablename__ = "m_citescore"
    __table_args__ = get_table_args(
        Index("idx_cs_agno", "agno"),
        Index("idx_cs_idFuente", "idFuente"),
        Index("idx_cs_issn", "issn"),
        Index("idx_cs_issn_combo", "issn", "agno", "categoria", unique=True),
        Index("idx_cs_revista", "revista"),
        Index("idx_cs_revista_agno", "revista", "agno"),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    revista: Mapped[str] = mapped_column(String(500), nullable=False)
    issn: Mapped[str] = mapped_column(String(9), nullable=False)
    agno: Mapped[str] = mapped_column(String(4), nullable=False)
    categoria: Mapped[str] = mapped_column(String(75), nullable=False)
    citeScore: Mapped[decimal.Decimal] = mapped_column(
        Numeric(6, 3), nullable=False, server_default=text("0.000")
    )
    posicion: Mapped[str | None] = mapped_column(String(15))
    cuartil: Mapped[str | None] = mapped_column(String(2))
    decil: Mapped[str | None] = mapped_column(String(3))
    tercil: Mapped[str | None] = mapped_column(String(2))
    idFuente: Mapped[int | None] = mapped_column(Integer)


class MCsicORM(Base):
    __tablename__ = "m_csic"
    __table_args__ = get_table_args(
        Index("idx_csic_editorial", "editorial"), schema_name=SCHEMA_NAME
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    editorial: Mapped[str | None] = mapped_column(String(179))
    puntuacion: Mapped[str | None] = mapped_column(String(5))


class MFecytORM(Base):
    __tablename__ = "m_fecyt"
    __table_args__ = get_table_args(
        Index("idx_fecyt_agno", "agno"),
        Index("idx_fecyt_eissn", "eissn"),
        Index("idx_fecyt_issn", "issn"),
        Index("idx_fecyt_issn_eissn", "issn", "eissn"),
        Index(
            "idx_fecyt_unique_combo",
            "titulo",
            "convocatoria",
            "agno",
            "categoria",
            unique=True,
        ),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    titulo: Mapped[str | None] = mapped_column(String(160))
    issn: Mapped[str | None] = mapped_column(String(11))
    eissn: Mapped[str | None] = mapped_column(String(9))
    url: Mapped[str | None] = mapped_column(String(133))
    convocatoria: Mapped[str | None] = mapped_column(String(22))
    igualdad: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(TINYINT(1), "mysql", "mariadb")
    )
    agno: Mapped[str | None] = mapped_column(String(4))
    categoria: Mapped[str | None] = mapped_column(String(52))
    puntuacion: Mapped[decimal.Decimal | None] = mapped_column(Numeric(4, 2))
    posicion: Mapped[str | None] = mapped_column(String(5))
    cuartil: Mapped[str | None] = mapped_column(String(2))


class MIdrORM(Base):
    __tablename__ = "m_idr"
    __table_args__ = get_table_args(
        Index("idx_idr_anualidad", "anualidad"),
        Index(
            "idx_idr_combo_unique",
            "anualidad",
            "categoria",
            "idFuente",
            unique=True,
        ),
        Index("idx_idr_categoria", "categoria"),
        Index("idx_idr_idFuente", "idFuente"),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    prisma_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    totalRevista: Mapped[int] = mapped_column(Integer, nullable=False)
    titulo: Mapped[str | None] = mapped_column(String(250))
    dialnet_id: Mapped[int | None] = mapped_column(Integer)
    issn: Mapped[str | None] = mapped_column(String(9))
    anualidad: Mapped[int | None] = mapped_column(Integer)
    categoria: Mapped[str | None] = mapped_column(String(50))
    factorImpacto: Mapped[decimal.Decimal | None] = mapped_column(Numeric(4, 3))
    cuartil: Mapped[int | None] = mapped_column(Integer)
    percentil: Mapped[int | None] = mapped_column(Integer)
    posicion: Mapped[int | None] = mapped_column(Integer)
    idFuente: Mapped[int | None] = mapped_column(Integer)


class MInformesORM(Base):
    __tablename__ = "m_informes"
    __table_args__ = get_table_args(
        Index("idx_inf_ambito", "ambito"),
        Index("idx_inf_ambito_tipo", "ambito", "tipo"),
        Index("idx_inf_basedatos_tipo", "basedatos", "tipo"),
        Index("idx_inf_identificador", "identificador"),
        Index("idx_inf_tipo", "tipo"),
        schema_name=SCHEMA_NAME,
    )

    idMetrica: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    ambito: Mapped[str] = mapped_column(String(15), nullable=False)
    identificador: Mapped[str] = mapped_column(String(10), nullable=False)
    basedatos: Mapped[str] = mapped_column(String(10), nullable=False)
    tipo: Mapped[str] = mapped_column(String(15), nullable=False)
    valor: Mapped[str] = mapped_column(String(20), nullable=False)
    fechaActualizacion: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    identificadorInt: Mapped[int | None] = mapped_column(Integer)


class MJciORM(Base):
    __tablename__ = "m_jci"
    __table_args__ = get_table_args(
        Index("idx_jci_agno", "agno"),
        Index("idx_jci_combo_unique", "agno", "categoria", "idFuente", unique=True),
        Index("idx_jci_idFuente", "idFuente"),
        Index("idx_jci_issn", "issn"),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    revista: Mapped[str] = mapped_column(String(500), nullable=False)
    agno: Mapped[str] = mapped_column(String(4), nullable=False)
    categoria: Mapped[str] = mapped_column(String(75), nullable=False)
    jci: Mapped[decimal.Decimal] = mapped_column(
        Numeric(6, 3), nullable=False, server_default=text("0.000")
    )
    percentil: Mapped[str] = mapped_column(String(10), nullable=False)
    issn: Mapped[str | None] = mapped_column(String(9))
    issn_2: Mapped[str | None] = mapped_column(String(9))
    posicion: Mapped[str | None] = mapped_column(String(15))
    cuartil: Mapped[str | None] = mapped_column(String(2))
    decil: Mapped[str | None] = mapped_column(String(3))
    tercil: Mapped[str | None] = mapped_column(String(2))
    idFuente: Mapped[int | None] = mapped_column(Integer)


class MJcrORM(Base):
    __tablename__ = "m_jcr"
    __table_args__ = get_table_args(
        Index("idx_jcr_category", "category"),
        Index("idx_jcr_decil", "decil"),
        Index("idx_jcr_idFuente", "idFuente"),
        Index("idx_jcr_impact_factor", "impact_factor"),
        Index("idx_jcr_issn_year", "issn", "year"),
        Index("idx_jcr_issn_year_edition", "issn", "year", "edition"),
        Index("idx_jcr_issn2_year", "issn_2", "year"),
        Index("idx_jcr_issn2_year_edition", "issn_2", "year", "edition"),
        Index("idx_jcr_journal", "journal"),
        Index("idx_jcr_journal_year", "journal", "year"),
        Index("idx_jcr_journal_issns", "journal", "issn", "issn_2"),
        Index("idx_jcr_quartile", "quartile"),
        Index("idx_jcr_tercil", "tercil"),
        Index("idx_jcr_year", "year"),
        Index(
            "idx_jcr_combo_unique",
            "year",
            "edition",
            "category",
            "idFuente",
            unique=True,
        ),
        schema_name=SCHEMA_NAME,
    )

    id_jcr: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    journal: Mapped[str] = mapped_column(String(500), nullable=False)
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    edition: Mapped[str] = mapped_column(String(100), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    issn: Mapped[str | None] = mapped_column(String(9))
    issn_2: Mapped[str | None] = mapped_column(String(9))
    impact_factor: Mapped[decimal.Decimal | None] = mapped_column(
        Numeric(6, 3), server_default=text("0.000")
    )
    rank: Mapped[str | None] = mapped_column(String(10))
    quartile: Mapped[str | None] = mapped_column(String(10))
    decil: Mapped[str | None] = mapped_column(String(8))
    tercil: Mapped[str | None] = mapped_column(String(8))
    percentile: Mapped[str | None] = mapped_column(String(10))
    idFuente: Mapped[int | None] = mapped_column(Integer)


class MPublicacionesORM(Base):
    __tablename__ = "m_publicaciones"
    __table_args__ = get_table_args(
        Index("idxPublicacion", "idPublicacion"), schema_name=SCHEMA_NAME
    )

    idMetrica: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    idPublicacion: Mapped[int] = mapped_column(Integer, nullable=False)
    metrica: Mapped[str] = mapped_column(String(25), nullable=False)
    basedatos: Mapped[str] = mapped_column(String(10), nullable=False)
    valor: Mapped[str] = mapped_column(String(25), nullable=False)
    fechaActualizacion: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class MSjrORM(Base):
    __tablename__ = "m_sjr"
    __table_args__ = get_table_args(
        Index("idx_sjr_category", "category"),
        Index("idx_sjr_decil", "decil"),
        Index("idx_sjr_idFuente", "idFuente"),
        Index("idx_sjr_impact_factor", "impact_factor"),
        Index("idx_sjr_issn_year", "issn", "year"),
        Index("idx_sjr_issn2_year_cat", "issn", "year", "category"),
        Index("idx_sjr_issn2_year", "issn_2", "year"),
        Index("idx_sjr_issn2_year_cat2", "issn_2", "year", "category"),
        Index("idx_sjr_journal", "journal"),
        Index("idx_sjr_quartile", "quartile"),
        Index("idx_sjr_tercil", "tercil"),
        Index("idx_sjr_year", "year"),
        Index("idx_sjr_year_idFuente", "year", "idFuente"),
        schema_name=SCHEMA_NAME,
    )

    id_sjr: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    journal: Mapped[str | None] = mapped_column(String(500))
    issn: Mapped[str | None] = mapped_column(String(9))
    issn_2: Mapped[str | None] = mapped_column(String(9))
    impact_factor: Mapped[decimal.Decimal | None] = mapped_column(
        Numeric(6, 3), server_default=text("0.000")
    )
    rank: Mapped[str | None] = mapped_column(String(10))
    quartile: Mapped[str | None] = mapped_column(String(10))
    decil: Mapped[str | None] = mapped_column(String(5))
    tercil: Mapped[str | None] = mapped_column(String(5))
    idFuente: Mapped[int | None] = mapped_column(Integer)


class MSpiORM(Base):
    __tablename__ = "m_spi"
    __table_args__ = get_table_args(
        Index("idx_spi_agno", "agno"),
        Index("idx_spi_editorial", "editorial"),
        schema_name=SCHEMA_NAME,
    )

    idMetrica: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    editorial: Mapped[str] = mapped_column(String(200), nullable=False)
    agno: Mapped[str] = mapped_column(
        String(4), nullable=False, server_default=text("'2018'")
    )
    categoria: Mapped[str] = mapped_column(String(35), nullable=False)
    puntuacion: Mapped[float] = mapped_column(Float, nullable=False)
    ambito: Mapped[str] = mapped_column(String(20), nullable=False)
    posicion: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    total_ed: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    cuartil: Mapped[str] = mapped_column(String(2), nullable=False)
    editorial_original: Mapped[str | None] = mapped_column(String(200))
