import datetime

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.mysql import MEDIUMINT, TIMESTAMP, TINYINT
from sqlalchemy.orm import Mapped, mapped_column

from v0_1.infrastructure.database.orm.base import Base, get_table_args

SCHEMA_NAME = "prisma"


# ---------------------------------------------------------------------------
# Declarative ORM Models
# ---------------------------------------------------------------------------


class IAreaORM(Base):
    __tablename__ = "i_area"
    __table_args__ = get_table_args(
        Index("idxRama", "idRama"),
        Index("nombre_UNIQUE_area", "nombre"),
        schema_name=SCHEMA_NAME,
    )

    idArea: Mapped[int] = mapped_column(
        Integer().with_variant(
            MEDIUMINT(display_width=3, unsigned=True, zerofill=True), "mysql", "mariadb"
        ),
        primary_key=True,
    )
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    idRama: Mapped[int] = mapped_column(
        Integer().with_variant(
            TINYINT(display_width=3, unsigned=True), "mysql", "mariadb"
        ),
    )


class IBibliotecaORM(Base):
    __tablename__ = "i_biblioteca"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idBiblioteca: Mapped[int] = mapped_column(
        SmallInteger().with_variant(
            TINYINT(display_width=2, unsigned=True), "mysql", "mariadb"
        ),
        primary_key=True,
    )
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)


class ICategoriaORM(Base):
    __tablename__ = "i_categoria"
    __table_args__ = get_table_args(
        Index("idx_femenino", "femenino"),
        Index("idx_categoria_nombre", "nombre"),
        schema_name=SCHEMA_NAME,
    )

    idCategoria: Mapped[str] = mapped_column(
        String(8), primary_key=True, server_default=text("''")
    )
    tipo_pp: Mapped[str] = mapped_column(
        String(3),
        nullable=False,
        server_default=text("'exc'"),
        comment="Tipo de usuario para los informes del plan propio: exc, cat, mie, pre, pos",
    )
    nombre: Mapped[str | None] = mapped_column(String(50))
    femenino: Mapped[str | None] = mapped_column(String(50))


class ICentroORM(Base):
    __tablename__ = "i_centro"
    __table_args__ = get_table_args(
        Index("biblioteca_idx", "idBiblioteca"),
        Index("idx_centro_nombre", "nombre"),
        schema_name=SCHEMA_NAME,
    )

    idCentro: Mapped[str] = mapped_column(String(5), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    idBiblioteca: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(
            TINYINT(display_width=2, unsigned=True), "mysql", "mariadb"
        ),
        server_default=text("0"),
    )
    encargado: Mapped[int | None] = mapped_column(Integer)


class ICentroMixtoORM(Base):
    __tablename__ = "i_centro_mixto"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idCentroMixto: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(500), nullable=False)
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    acronimo: Mapped[str | None] = mapped_column(String(20))
    ambito: Mapped[str | None] = mapped_column(String(100))
    url: Mapped[str | None] = mapped_column(Text)
    fecha_creacion: Mapped[datetime.date | None] = mapped_column(Date)


class ICentroMixtoLineaInvestigacionORM(Base):
    __tablename__ = "i_centro_mixto_linea_investigacion"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idCentroMixto: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class ICentroMixtoPalabraClaveORM(Base):
    __tablename__ = "i_centro_mixto_palabra_clave"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idCentroMixto: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    )
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IConjuntoORM(Base):
    __tablename__ = "i_conjunto"
    __table_args__ = get_table_args(
        Index("idx_conjunto_tipo", "tipo"), schema_name=SCHEMA_NAME
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tipo: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="Instituto, Unidad de Excelencia, etc."
    )
    nombre: Mapped[str] = mapped_column(String(500), nullable=False)
    acronimo: Mapped[str | None] = mapped_column(String(20), comment="Nombre acortado")
    responsable_id: Mapped[int | None] = mapped_column(
        Integer, comment="Identificador del investigador responsable"
    )


class IDepartamentoORM(Base):
    __tablename__ = "i_departamento"
    __table_args__ = get_table_args(
        Index("idx_departamento_nombre", "nombre"), schema_name=SCHEMA_NAME
    )

    idDepartamento: Mapped[str] = mapped_column(String(4), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)


class IDoctoradoORM(Base):
    __tablename__ = "i_doctorado"
    __table_args__ = get_table_args(
        Index("idx_doctorado_nombre", "nombre"), schema_name=SCHEMA_NAME
    )

    idDoctorado: Mapped[int] = mapped_column(
        SmallInteger, primary_key=True, server_default=text("0")
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    nombre: Mapped[str | None] = mapped_column(String(250))


class IDoctoradoLineaInvestigacionORM(Base):
    __tablename__ = "i_doctorado_linea_investigacion"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idDoctorado: Mapped[int] = mapped_column(Integer, primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(Integer, primary_key=True)


class IFechaCeseORM(Base):
    __tablename__ = "i_fecha_cese"
    __table_args__ = get_table_args(
        Index("fk_motivocese_investigador_idx", "idInvestigador"),
        Index("fk_motivocese_idx", "idMotivo"),
        Index("idInves_UNIQUE", "idInvestigador", unique=True),
        schema_name=SCHEMA_NAME,
    )

    idInvestigador: Mapped[int] = mapped_column(Integer, primary_key=True)
    idMotivo: Mapped[str] = mapped_column(String(8), primary_key=True)
    fechaCese: Mapped[datetime.date] = mapped_column(Date, nullable=False)


class IGrupoORM(Base):
    __tablename__ = "i_grupo"
    __table_args__ = get_table_args(
        Index("idx_grupo_acronimo", "acronimo"),
        Index("idx_grupo_nombre", "nombre"),
        schema_name=SCHEMA_NAME,
    )

    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    ambito: Mapped[str] = mapped_column(
        String(100), nullable=False, server_default=text("'Andalucía'")
    )
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    acronimo: Mapped[str | None] = mapped_column(
        String(75), comment="Acrónimo del grupo (viene de SISIUS)"
    )
    rama: Mapped[str | None] = mapped_column(
        String(4),
        comment="Rama científica en la que se engloba el grupo (viene de SISIUS)",
    )
    codigo: Mapped[int | None] = mapped_column(SmallInteger)
    institucion: Mapped[str | None] = mapped_column(String(200))
    fecha_creacion: Mapped[datetime.date | None] = mapped_column(Date)
    estado: Mapped[str | None] = mapped_column(String(100))
    situacion: Mapped[str | None] = mapped_column(String(100))


class IGrupoInvestigadorORM(Base):
    __tablename__ = "i_grupo_investigador"
    __table_args__ = get_table_args(
        Index("idx_gi_investigador", "idInvestigador", unique=True),
        schema_name=SCHEMA_NAME,
    )

    idInvestigador: Mapped[int] = mapped_column(Integer, primary_key=True)
    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    rol: Mapped[str | None] = mapped_column(
        String(50), server_default=text("'Miembro'")
    )
    actualizado: Mapped[datetime.datetime | None] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        server_default=func.now(),
        onupdate=func.now(),
    )


class IGrupoLineaInvestigacionORM(Base):
    __tablename__ = "i_grupo_linea_investigacion"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IGrupoPalabraClaveORM(Base):
    __tablename__ = "i_grupo_palabra_clave"
    __table_args__ = get_table_args(
        Index("idx_gpc_grupo", "idGrupo"),
        Index("idx_gpc_palabra", "idPalabraClave"),
        schema_name=SCHEMA_NAME,
    )

    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    )
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IIdentificadorInvestigadorORM(Base):
    __tablename__ = "i_identificador_investigador"
    __table_args__ = get_table_args(
        Index("idx_ii_investigador", "idInvestigador"),
        Index("idx_ii_tipo", "tipo"),
        Index("idx_ii_tipo_valor_unique", "tipo", "valor", unique=True),
        Index("idx_ii_valor", "valor"),
        schema_name=SCHEMA_NAME,
    )

    idIdentificador: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    idInvestigador: Mapped[int] = mapped_column(Integer, nullable=False)
    tipo: Mapped[str] = mapped_column(
        String(45),
        nullable=False,
        comment="orcid, wos, scopus, dialnet, idus, scholar",
    )
    valor: Mapped[str] = mapped_column(String(100), nullable=False)
    comentario: Mapped[str | None] = mapped_column(String(200))
    eliminado: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        server_default=text("0"),
    )


class IInstitucionColectivoORM(Base):
    __tablename__ = "i_institucion_colectivo"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idInstitucion: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    idColectivo: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tipo: Mapped[str] = mapped_column(String(50), primary_key=True)


class IInstitutoORM(Base):
    __tablename__ = "i_instituto"
    __table_args__ = get_table_args(
        Index("idx_inst_acronimo", "acronimo"),
        Index("idx_inst_nombre", "nombre"),
        schema_name=SCHEMA_NAME,
    )

    idInstituto: Mapped[int] = mapped_column(
        SmallInteger().with_variant(
            TINYINT(display_width=2, unsigned=True), "mysql", "mariadb"
        ),
        primary_key=True,
        autoincrement=True,
    )
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    acronimo: Mapped[int] = mapped_column(String(20), nullable=False)
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    ambito: Mapped[str | None] = mapped_column(String(100))
    url: Mapped[str | None] = mapped_column(Text)
    fecha_creacion: Mapped[datetime.date | None] = mapped_column(Date)


class IInstitutoLineaInvestigacionORM(Base):
    __tablename__ = "i_instituto_linea_investigacion"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idInstituto: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IInstitutoPalabraClaveORM(Base):
    __tablename__ = "i_instituto_palabra_clave"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idInstituto: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    )
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class InvestigadorORM(Base):
    __tablename__ = "i_investigador"
    __table_args__ = get_table_args(
        Index("idx_inv_apellidos", "apellidos"),
        Index("idx_inv_docuIden_UNIQUE", "docuIden", unique=True),
        Index("idx_inv_email_UNIQUE", "email", unique=True),
        Index("idx_inv_idCentroCenso", "idCentroCenso"),
        Index("idx_inv_idArea", "idArea"),
        Index("idx_inv_idCategoria", "idCategoria"),
        Index("idx_inv_idCentro", "idCentro"),
        Index("idx_inv_idDepartamento", "idDepartamento"),
        Index("idx_inv_nombre", "nombre"),
        schema_name=SCHEMA_NAME,
    )

    idInvestigador: Mapped[int] = mapped_column(
        Integer,
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
        SmallInteger().with_variant(
            MEDIUMINT(display_width=3, unsigned=True, zerofill=True), "mysql", "mariadb"
        ),
        nullable=False,
    )
    idDepartamento: Mapped[str] = mapped_column(String(4), nullable=False)
    idCentro: Mapped[str] = mapped_column(String(5), nullable=False)
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    perfilPublico: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("1"),
    )
    email: Mapped[str | None] = mapped_column(
        String(75),
        comment="Correo electrónico del investigador. Debería ser del dominio us.es",
    )
    fechaContratacion: Mapped[datetime.date | None] = mapped_column(
        Date, comment="Primera fecha de contratación"
    )
    idCentroCenso: Mapped[str | None] = mapped_column(String(5))
    nacionalidad: Mapped[str | None] = mapped_column(String(30))
    sexo: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=4), "mysql", "mariadb")
    )
    fechaNacimiento: Mapped[datetime.date | None] = mapped_column(Date)
    fechaNombramiento: Mapped[datetime.date | None] = mapped_column(Date)
    fechaActualizacion: Mapped[datetime.datetime | None] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        server_default=func.now(),
        onupdate=func.now(),
        comment="Fecha de la última actualización del registro",
    )


class IInvestigadorLineaInvestigacionORM(Base):
    __tablename__ = "i_investigador_linea_investigacion"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idInvestigador: Mapped[int] = mapped_column(Integer, primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IInvestigadorPalabraClaveORM(Base):
    __tablename__ = "i_investigador_palabra_clave"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idInvestigador: Mapped[int] = mapped_column(Integer, primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class ILineaInvestigacionORM(Base):
    __tablename__ = "i_linea_investigacion"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idLineaInvestigacion: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(Text, nullable=False)
    fecha: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
    )


class ILineaInvestigacionDoctoradoORM(Base):
    __tablename__ = "i_linea_investigacion_doctorado"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idLineaInvestigacion: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(500), nullable=False)


class IMiembroCentroMixtoORM(Base):
    __tablename__ = "i_miembro_centro_mixto"
    __table_args__ = get_table_args(
        Index("idx_mcm_investigador", "idInvestigador"),
        Index("idx_mcm_centro", "idCentroMixto"),
        Index("idx_mcm_rol", "rol"),
        schema_name=SCHEMA_NAME,
    )

    idInvestigador: Mapped[int] = mapped_column(Integer, primary_key=True)
    idCentroMixto: Mapped[int] = mapped_column(
        SmallInteger().with_variant(
            TINYINT(display_width=2, unsigned=True), "mysql", "mariadb"
        ),
        nullable=False,
    )
    rol: Mapped[str] = mapped_column(
        String(100), nullable=False, server_default=text("'Miembro ordinario'")
    )
    actualizado: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("1"),
    )


class IMiembroConjuntoORM(Base):
    __tablename__ = "i_miembro_conjunto"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    investigador_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    conjunto_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)


class IMiembroInstitutoORM(Base):
    __tablename__ = "i_miembro_instituto"
    __table_args__ = get_table_args(
        Index("idx_mi_investigador", "idInvestigador"),
        Index("idx_mi_instituto", "idInstituto"),
        Index("idx_mi_rol", "rol"),
        schema_name=SCHEMA_NAME,
    )

    idInvestigador: Mapped[int] = mapped_column(Integer, primary_key=True)
    idInstituto: Mapped[int] = mapped_column(
        SmallInteger().with_variant(
            TINYINT(display_width=2, unsigned=True), "mysql", "mariadb"
        ),
        nullable=False,
    )
    rol: Mapped[str] = mapped_column(
        String(100), nullable=False, server_default=text("'Miembro ordinario'")
    )
    actualizado: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("1"),
    )


class IMiembroUnidadExcelenciaORM(Base):
    __tablename__ = "i_miembro_unidad_excelencia"
    __table_args__ = get_table_args(
        Index("idx_mue_investigador", "idInvestigador"),
        Index("idx_mue_unidad", "idUdExcelencia"),
        Index("idx_mue_rol", "rol"),
        schema_name=SCHEMA_NAME,
    )

    idInvestigador: Mapped[int] = mapped_column(Integer, primary_key=True)
    idUdExcelencia: Mapped[int] = mapped_column(
        SmallInteger().with_variant(
            TINYINT(display_width=2, unsigned=True), "mysql", "mariadb"
        ),
        nullable=False,
    )
    rol: Mapped[str] = mapped_column(
        String(100), nullable=False, server_default=text("'Miembro ordinario'")
    )
    actualizado: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("1"),
    )


class IMotivoCeseORM(Base):
    __tablename__ = "i_motivo_cese"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idMotivo: Mapped[str] = mapped_column(String(8), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(250), nullable=False)


class IPalabraClaveORM(Base):
    __tablename__ = "i_palabra_clave"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idPalabraClave: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    fecha: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
    )


class IProfesorDoctoradoORM(Base):
    __tablename__ = "i_profesor_doctorado"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idInvestigador: Mapped[int] = mapped_column(
        Integer, primary_key=True, server_default=text("0")
    )
    idDoctorado: Mapped[int] = mapped_column(
        SmallInteger, primary_key=True, server_default=text("0")
    )
    actualizado: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
    )


class IProfesorDoctoradoLineaInvORM(Base):
    __tablename__ = "i_profesor_doctorado_linea_inv"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idInvestigador: Mapped[int] = mapped_column(
        Integer, primary_key=True, server_default=text("0")
    )
    idLineaInvestigacion: Mapped[int] = mapped_column(
        Integer, primary_key=True, server_default=text("0")
    )
    actualizado: Mapped[datetime.datetime] = mapped_column(
        DateTime().with_variant(TIMESTAMP(), "mysql", "mariadb"),
        nullable=False,
        server_default=func.now(),
    )


class IRamaORM(Base):
    __tablename__ = "i_rama"
    __table_args__ = get_table_args(
        Index("fk_i_rama_padre_idx", "padre"),
        Index("nombre_UNIQUE_rama", "nombre", unique=True),
        schema_name=SCHEMA_NAME,
    )

    idRama: Mapped[int] = mapped_column(
        SmallInteger().with_variant(
            TINYINT(display_width=2, unsigned=True), "mysql", "mariadb"
        ),
        primary_key=True,
        autoincrement=True,
    )
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    padre: Mapped[int | None] = mapped_column(
        SmallInteger().with_variant(
            TINYINT(display_width=2, unsigned=True), "mysql", "mariadb"
        ),
        server_default=text("0"),
        comment="0 cuando es una rama fundamental",
    )


class IRamaUsORM(Base):
    __tablename__ = "i_rama_us"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idDepartamento: Mapped[str] = mapped_column(String(4), primary_key=True)
    idArea: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    idRama: Mapped[int] = mapped_column(
        SmallInteger().with_variant(
            TINYINT(display_width=2, unsigned=True), "mysql", "mariadb"
        ),
        primary_key=True,
        server_default=text("0"),
    )


class ISexenioORM(Base):
    __tablename__ = "i_sexenio"
    __table_args__ = get_table_args(
        Index("idx_sexenio_inicio_fin", "inicio", "fin"),
        Index("idx_sexenio_investigador", "investigador_id"),
        Index(
            "idx_sexenio_unique_combo",
            "investigador_id",
            "inicio",
            "fin",
            "transferencia",
            unique=True,
        ),
        schema_name=SCHEMA_NAME,
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    investigador_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    inicio: Mapped[str] = mapped_column(String(4), nullable=False)
    fin: Mapped[str] = mapped_column(String(4), nullable=False)
    transferencia: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
    )
    entrada_vigor: Mapped[str] = mapped_column(String(4), nullable=False)
    id_categoria: Mapped[str] = mapped_column(String(8), nullable=False)
    nomina: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
    )


class IUnidadExcelenciaORM(Base):
    __tablename__ = "i_unidad_excelencia"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idUdExcelencia: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(500), nullable=False)
    resumen: Mapped[str] = mapped_column(Text, nullable=False)
    fecha_creacion: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    acronimo: Mapped[str | None] = mapped_column(String(20), comment="Nombre acortado")
    ambito: Mapped[str | None] = mapped_column(String(100))
    url: Mapped[str | None] = mapped_column(Text)


class IUnidadExcelenciaLineaInvestigacionORM(Base):
    __tablename__ = "i_unidad_excelencia_linea_investigacion"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idUdExcelencia: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IUnidadExcelenciaPalabraClaveORM(Base):
    __tablename__ = "i_unidad_excelencia_palabra_clave"
    __table_args__ = get_table_args(schema_name=SCHEMA_NAME)

    idUdExcelencia: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        SmallInteger().with_variant(TINYINT(display_width=1), "mysql", "mariadb"),
        nullable=False,
        server_default=text("0"),
    )
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class InstitucionORM(Base):
    __tablename__ = "institucion"
    __table_args__ = get_table_args(
        Index("idx_institucion_ror", "id_ror", unique=True), schema_name=SCHEMA_NAME
    )

    idInstitucion: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    id_ror: Mapped[str | None] = mapped_column(String(9))
    tipo_organizacion: Mapped[str | None] = mapped_column(String(50))
    enlace: Mapped[str | None] = mapped_column(String(250))
    acronimos: Mapped[str | None] = mapped_column(String(100))
    coordenadas_lat: Mapped[str | None] = mapped_column("coordenadas.lat", String(40))
    coordenadas_lng: Mapped[str | None] = mapped_column("coordenadas.lng", String(40))
    ciudad: Mapped[str | None] = mapped_column(String(75))
    pais: Mapped[str | None] = mapped_column(String(75))
    fundref_preferido: Mapped[str | None] = mapped_column(String(75))
