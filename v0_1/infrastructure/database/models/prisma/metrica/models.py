import datetime
import decimal

from sqlalchemy import (
    DECIMAL,
    TIMESTAMP,
    Column,
    Date,
    Float,
    Index,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.mysql import BIGINT, INTEGER, SMALLINT, TINYINT, VARCHAR
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.models.base import Base

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
    schema="prisma",
)


class MCeaApq(Base):
    __tablename__ = "m_cea_apq"
    __table_args__ = (
        Index("monografia", "monografia"),
        {"schema": "prisma"},
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    idFuente: Mapped[int] = mapped_column(INTEGER(10), nullable=False)
    monografia: Mapped[int] = mapped_column(
        TINYINT(4), nullable=False, server_default=text("0")
    )
    coleccion: Mapped[str | None] = mapped_column(String(200))
    universidad: Mapped[str | None] = mapped_column(String(160))
    convocatoria: Mapped[str | None] = mapped_column(String(22))
    agno: Mapped[str | None] = mapped_column(String(4))
    internacionalidad: Mapped[int | None] = mapped_column(TINYINT(1))
    fecha_expiracion: Mapped[datetime.date | None] = mapped_column(Date)
    url: Mapped[str | None] = mapped_column(
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
        {"schema": "prisma"},
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
    posicion: Mapped[str | None] = mapped_column(
        VARCHAR(15, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    cuartil: Mapped[str | None] = mapped_column(
        VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    decil: Mapped[str | None] = mapped_column(
        VARCHAR(3, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    tercil: Mapped[str | None] = mapped_column(
        VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    idFuente: Mapped[int | None] = mapped_column(INTEGER(11))


class MCsic(Base):
    __tablename__ = "m_csic"
    __table_args__ = (
        Index("editorial", "editorial"),
        {"schema": "prisma"},
    )

    id: Mapped[int] = mapped_column(INTEGER(11), primary_key=True, autoincrement=True)
    editorial: Mapped[str | None] = mapped_column(String(179))
    puntuacion: Mapped[str | None] = mapped_column(String(5))


class MFecyt(Base):
    __tablename__ = "m_fecyt"
    __table_args__ = (
        Index("agno", "agno"),
        Index("eissn", "eissn"),
        Index("issn", "issn"),
        Index("issn_2", "issn", "eissn"),
        Index("titulo", "titulo", "convocatoria", "agno", "categoria", unique=True),
        {"schema": "prisma"},
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    titulo: Mapped[str | None] = mapped_column(String(160))
    issn: Mapped[str | None] = mapped_column(String(11))
    eissn: Mapped[str | None] = mapped_column(String(9))
    url: Mapped[str | None] = mapped_column(String(133))
    convocatoria: Mapped[str | None] = mapped_column(String(22))
    igualdad: Mapped[int | None] = mapped_column(TINYINT(1))
    agno: Mapped[str | None] = mapped_column(String(4))
    categoria: Mapped[str | None] = mapped_column(String(52))
    puntuacion: Mapped[decimal.Decimal | None] = mapped_column(DECIMAL(4, 2))
    posicion: Mapped[str | None] = mapped_column(String(5))
    cuartil: Mapped[str | None] = mapped_column(String(2))


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
        {"schema": "prisma"},
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    prisma_id: Mapped[int] = mapped_column(BIGINT(20), nullable=False)
    totalRevista: Mapped[int] = mapped_column(INTEGER(11), nullable=False)
    titulo: Mapped[str | None] = mapped_column(String(250))
    dialnet_id: Mapped[int | None] = mapped_column(INTEGER(11))
    issn: Mapped[str | None] = mapped_column(String(9))
    anualidad: Mapped[int | None] = mapped_column(INTEGER(11))
    categoria: Mapped[str | None] = mapped_column(String(50))
    factorImpacto: Mapped[decimal.Decimal | None] = mapped_column(DECIMAL(4, 3))
    cuartil: Mapped[int | None] = mapped_column(INTEGER(11))
    percentil: Mapped[int | None] = mapped_column(INTEGER(11))
    posicion: Mapped[int | None] = mapped_column(INTEGER(11))
    idFuente: Mapped[int | None] = mapped_column(INTEGER(11))


class MInformes(Base):
    __tablename__ = "m_informes"
    __table_args__ = (
        Index("ambito", "ambito"),
        Index("ambito_2", "ambito", "tipo"),
        Index("basedatos", "basedatos", "tipo"),
        Index("identificador", "identificador"),
        Index("tipo", "tipo"),
        {"schema": "prisma"},
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
    identificadorInt: Mapped[int | None] = mapped_column(INTEGER(11))


class MJci(Base):
    __tablename__ = "m_jci"
    __table_args__ = (
        Index("agno", "agno"),
        Index("agno_2", "agno", "categoria", "idFuente", unique=True),
        Index("idFuente", "idFuente"),
        Index("issn", "issn"),
        {"schema": "prisma"},
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
    issn: Mapped[str | None] = mapped_column(
        VARCHAR(9, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    issn_2: Mapped[str | None] = mapped_column(String(9))
    posicion: Mapped[str | None] = mapped_column(
        VARCHAR(15, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    cuartil: Mapped[str | None] = mapped_column(
        VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    decil: Mapped[str | None] = mapped_column(
        VARCHAR(3, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    tercil: Mapped[str | None] = mapped_column(
        VARCHAR(2, charset="utf8mb3", collation="utf8mb3_spanish_ci")
    )
    idFuente: Mapped[int | None] = mapped_column(INTEGER(11))


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
        {"schema": "prisma"},
    )

    id_jcr: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    journal: Mapped[str] = mapped_column(String(500), nullable=False)
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    edition: Mapped[str] = mapped_column(String(100), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    issn: Mapped[str | None] = mapped_column(String(9))
    issn_2: Mapped[str | None] = mapped_column(String(9))
    impact_factor: Mapped[decimal.Decimal | None] = mapped_column(
        DECIMAL(6, 3), server_default=text("0.000")
    )
    rank: Mapped[str | None] = mapped_column(String(10))
    quartile: Mapped[str | None] = mapped_column(String(10))
    decil: Mapped[str | None] = mapped_column(String(8))
    tercil: Mapped[str | None] = mapped_column(String(8))
    percentile: Mapped[str | None] = mapped_column(String(10))
    idFuente: Mapped[int | None] = mapped_column(INTEGER(11))


class MPublicaciones(Base):
    __tablename__ = "m_publicaciones"
    __table_args__ = (
        Index("idxPublicacion", "idPublicacion"),
        {"comment": "Métricas de las publicaciones"},
        {"schema": "prisma"},
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
    schema="prisma",
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
    schema="prisma",
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
        {"schema": "prisma"},
    )

    id_sjr: Mapped[int] = mapped_column(
        INTEGER(10, unsigned=True), primary_key=True, autoincrement=True
    )
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    category: Mapped[str] = mapped_column(String(75), nullable=False)
    journal: Mapped[str | None] = mapped_column(String(500))
    issn: Mapped[str | None] = mapped_column(String(9))
    issn_2: Mapped[str | None] = mapped_column(String(9))
    impact_factor: Mapped[decimal.Decimal | None] = mapped_column(
        DECIMAL(6, 3), server_default=text("0.000")
    )
    rank: Mapped[str | None] = mapped_column(String(10))
    quartile: Mapped[str | None] = mapped_column(String(10))
    decil: Mapped[str | None] = mapped_column(String(5))
    tercil: Mapped[str | None] = mapped_column(String(5))
    idFuente: Mapped[int | None] = mapped_column(INTEGER(11))


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
    editorial_original: Mapped[str | None] = mapped_column(
        VARCHAR(200, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
