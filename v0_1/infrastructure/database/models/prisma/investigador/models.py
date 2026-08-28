import datetime

from sqlalchemy import (
    TIMESTAMP,
    Date,
    Index,
    String,
    Text,
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


class IArea(Base):
    __tablename__ = "i_area"
    __table_args__ = (
        Index("idxRama", "idRama"),
        Index("nombre_UNIQUE", "nombre", unique=True),
        {"schema": "prisma"},
    )

    idArea: Mapped[int] = mapped_column(
        SMALLINT(3, unsigned=True, zerofill=True), primary_key=True
    )
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    idRama: Mapped[int] = mapped_column(TINYINT(3, unsigned=True), nullable=False)


class IBiblioteca(Base):
    __tablename__ = "i_biblioteca"
    __table_args__ = {"schema": "prisma"}

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
        {"schema": "prisma"},
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
    nombre: Mapped[str | None] = mapped_column(String(50))
    femenino: Mapped[str | None] = mapped_column(String(50))


class ICentro(Base):
    __tablename__ = "i_centro"
    __table_args__ = (
        Index("biblioteca_idx", "idBiblioteca"),
        Index("nombre", "nombre"),
        {"schema": "prisma"},
    )

    idCentro: Mapped[str] = mapped_column(String(5), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    idBiblioteca: Mapped[int | None] = mapped_column(
        TINYINT(2, unsigned=True), server_default=text("0")
    )
    encargado: Mapped[int | None] = mapped_column(INTEGER(11))


class ICentroMixto(Base):
    __tablename__ = "i_centro_mixto"
    __table_args__ = {"schema": "prisma"}

    idCentroMixto: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(500), nullable=False)
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    acronimo: Mapped[str | None] = mapped_column(String(20))
    ambito: Mapped[str | None] = mapped_column(String(100))
    url: Mapped[str | None] = mapped_column(Text)
    fecha_creacion: Mapped[datetime.date | None] = mapped_column(Date)


class ICentroMixtoLineaInvestigacion(Base):
    __tablename__ = "i_centro_mixto_linea_investigacion"
    __table_args__ = {"schema": "prisma"}

    idCentroMixto: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class ICentroMixtoPalabraClave(Base):
    __tablename__ = "i_centro_mixto_palabra_clave"
    __table_args__ = {"schema": "prisma"}

    idCentroMixto: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IConjunto(Base):
    __tablename__ = "i_conjunto"
    __table_args__ = (
        Index("tipo", "tipo"),
        {"schema": "prisma"},
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    tipo: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="Instituto, Unidad de Excelencia, etc."
    )
    nombre: Mapped[str] = mapped_column(
        VARCHAR(500, charset="utf8mb4", collation="utf8mb4_spanish_ci"), nullable=False
    )
    acronimo: Mapped[str | None] = mapped_column(
        VARCHAR(20, charset="utf8mb4", collation="utf8mb4_spanish_ci"),
        comment="Nombre acortado",
    )
    responsable_id: Mapped[int | None] = mapped_column(
        INTEGER(11), comment="Identificador del investigador responsable"
    )


class IDepartamento(Base):
    __tablename__ = "i_departamento"
    __table_args__ = (
        Index("nombre", "nombre"),
        {"schema": "prisma"},
    )

    idDepartamento: Mapped[str] = mapped_column(String(4), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)


class IDoctorado(Base):
    __tablename__ = "i_doctorado"
    __table_args__ = (
        Index("nombre", "nombre", mysql_length={"nombre": 191}),
        {"schema": "prisma"},
    )

    idDoctorado: Mapped[int] = mapped_column(
        SMALLINT(4), primary_key=True, server_default=text("0")
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    nombre: Mapped[str | None] = mapped_column(
        VARCHAR(250, charset="utf8mb4", collation="utf8mb4_spanish_ci")
    )


class IDoctoradoLineaInvestigacion(Base):
    __tablename__ = "i_doctorado_linea_investigacion"
    __table_args__ = {"schema": "prisma"}

    idDoctorado: Mapped[int] = mapped_column(INTEGER(4), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(INTEGER(8), primary_key=True)


class IFechaCese(Base):
    __tablename__ = "i_fecha_cese"
    __table_args__ = (
        Index("fk_motivocese_has_investigador_investigador1_idx", "idInvestigador"),
        Index("fk_motivocese_has_investigador_motivocese1_idx", "idMotivo"),
        Index("idInves_UNIQUE", "idInvestigador", unique=True),
        {"schema": "prisma"},
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
        {"schema": "prisma"},
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
        comment="Rama cientifíca en la que se engloba el grupo de investigación (viene de SISISUS)",
    )
    codigo: Mapped[int | None] = mapped_column(SMALLINT(4))
    institucion: Mapped[str | None] = mapped_column(String(200))
    fecha_creacion: Mapped[datetime.date | None] = mapped_column(Date)
    estado: Mapped[str | None] = mapped_column(String(100))
    situacion: Mapped[str | None] = mapped_column(String(100))


class IGrupoInvestigador(Base):
    __tablename__ = "i_grupo_investigador"
    __table_args__ = (
        Index("idInvestigador", "idInvestigador", unique=True),
        {"schema": "prisma"},
    )

    idInvestigador: Mapped[int] = mapped_column(INTEGER(10), primary_key=True)
    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    rol: Mapped[str | None] = mapped_column(
        String(50), server_default=text("'Miembro'")
    )
    actualizado: Mapped[datetime.datetime | None] = mapped_column(TIMESTAMP)


class IGrupoLineaInvestigacion(Base):
    __tablename__ = "i_grupo_linea_investigacion"
    __table_args__ = {"schema": "prisma"}

    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IGrupoPalabraClave(Base):
    __tablename__ = "i_grupo_palabra_clave"
    __table_args__ = (
        Index("idGrupo", "idGrupo"),
        Index("idPalabraClave", "idPalabraClave"),
        {"schema": "prisma"},
    )

    idGrupo: Mapped[str] = mapped_column(String(10), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IIdentificadorInvestigador(Base):
    __tablename__ = "i_identificador_investigador"
    __table_args__ = (
        Index("investigador_idx", "idInvestigador"),
        Index("tipo", "tipo"),
        Index("tipo_2", "tipo", "valor", unique=True),
        Index("valor", "valor"),
        {"schema": "prisma"},
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
    comentario: Mapped[str | None] = mapped_column(String(200))
    eliminado: Mapped[int | None] = mapped_column(TINYINT(1), server_default=text("0"))


class IInstitucionColectivo(Base):
    __tablename__ = "i_institucion_colectivo"
    __table_args__ = {"schema": "prisma"}

    idInstitucion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idColectivo: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    tipo: Mapped[str] = mapped_column(String(50), primary_key=True)


class IInstituto(Base):
    __tablename__ = "i_instituto"
    __table_args__ = (
        Index("acronimo", "acronimo"),
        Index("nombre", "nombre", mysql_length={"nombre": 191}),
        {"schema": "prisma"},
    )

    idInstituto: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    acronimo: Mapped[str] = mapped_column(String(20), nullable=False)
    resumen: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("''")
    )
    ambito: Mapped[str | None] = mapped_column(String(100))
    url: Mapped[str | None] = mapped_column(Text)
    fecha_creacion: Mapped[datetime.date | None] = mapped_column(Date)


class IInstitutoLineaInvestigacion(Base):
    __tablename__ = "i_instituto_linea_investigacion"
    __table_args__ = {"schema": "prisma"}

    idInstituto: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IInstitutoPalabraClave(Base):
    __tablename__ = "i_instituto_palabra_clave"
    __table_args__ = {"schema": "prisma"}

    idInstituto: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


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
        {"schema": "prisma"},
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
    email: Mapped[str | None] = mapped_column(
        String(75),
        comment="Correo electrónico del investigador. Debería ser del dominio us.es",
    )
    fechaContratacion: Mapped[datetime.date | None] = mapped_column(
        Date, comment="Primera fecha de contratación"
    )
    idCentroCenso: Mapped[str | None] = mapped_column(String(5))
    nacionalidad: Mapped[str | None] = mapped_column(String(30))
    sexo: Mapped[int | None] = mapped_column(TINYINT(4))
    fechaNacimiento: Mapped[datetime.date | None] = mapped_column(Date)
    fechaNombramiento: Mapped[datetime.date | None] = mapped_column(Date)
    fechaActualizacion: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
        comment="Fecha de la última actualización del registro",
    )


class IInvestigadorLineaInvestigacion(Base):
    __tablename__ = "i_investigador_linea_investigacion"
    __table_args__ = {"schema": "prisma"}

    idInvestigador: Mapped[int] = mapped_column(INTEGER(10), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IInvestigadorPalabraClave(Base):
    __tablename__ = "i_investigador_palabra_clave"
    __table_args__ = {"schema": "prisma"}

    idInvestigador: Mapped[int] = mapped_column(INTEGER(10), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class ILineaInvestigacion(Base):
    __tablename__ = "i_linea_investigacion"
    __table_args__ = {"schema": "prisma"}

    idLineaInvestigacion: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(Text, nullable=False)
    fecha: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )


class ILineaInvestigacionDoctorado(Base):
    __tablename__ = "i_linea_investigacion_doctorado"
    __table_args__ = {"schema": "prisma"}

    idLineaInvestigacion: Mapped[int] = mapped_column(INTEGER(8), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(500), nullable=False)


class IMiembroCentroMixto(Base):
    __tablename__ = "i_miembro_centro_mixto"
    __table_args__ = (
        Index("idxInstituto", "idInvestigador"),
        Index("idxInvestigador", "idCentroMixto"),
        Index("rol", "rol"),
        {"schema": "prisma"},
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
    __table_args__ = {"schema": "prisma"}

    investigador_id: Mapped[int] = mapped_column(INTEGER(11), primary_key=True)
    conjunto_id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)


class IMiembroInstituto(Base):
    __tablename__ = "i_miembro_instituto"
    __table_args__ = (
        Index("idxInstituto", "idInvestigador"),
        Index("idxInvestigador", "idInstituto"),
        Index("rol", "rol"),
        {"schema": "prisma"},
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
        {"schema": "prisma"},
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
    __table_args__ = {"schema": "prisma"}

    idMotivo: Mapped[str] = mapped_column(String(8), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(250), nullable=False)


class IPalabraClave(Base):
    __tablename__ = "i_palabra_clave"
    __table_args__ = {"schema": "prisma"}

    idPalabraClave: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    fecha: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )


class IProfesorDoctorado(Base):
    __tablename__ = "i_profesor_doctorado"
    __table_args__ = {"schema": "prisma"}

    idInvestigador: Mapped[int] = mapped_column(
        INTEGER(10), primary_key=True, server_default=text("0")
    )
    idDoctorado: Mapped[int] = mapped_column(
        SMALLINT(4), primary_key=True, server_default=text("0")
    )
    actualizado: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=text("current_timestamp()")
    )


class IProfesorDoctoradoLineaInv(Base):
    __tablename__ = "i_profesor_doctorado_linea_inv"
    __table_args__ = {"schema": "prisma"}

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
        {"schema": "prisma"},
    )

    idRama: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    padre: Mapped[int | None] = mapped_column(
        TINYINT(2, unsigned=True),
        server_default=text("0"),
        comment="0 cuando es una rama fundamental",
    )


class IRamaUs(Base):
    __tablename__ = "i_rama_us"
    __table_args__ = {"schema": "prisma"}

    idDepartamento: Mapped[str] = mapped_column(String(4), primary_key=True)
    idArea: Mapped[int] = mapped_column(
        SMALLINT(3, unsigned=True, zerofill=True), primary_key=True
    )
    idRama: Mapped[int] = mapped_column(
        TINYINT(2, unsigned=True), primary_key=True, server_default=text("0")
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
        {"schema": "prisma"},
    )

    id: Mapped[int] = mapped_column(BIGINT(20), primary_key=True, autoincrement=True)
    investigador_id: Mapped[int] = mapped_column(BIGINT(20), nullable=False)
    inicio: Mapped[str] = mapped_column(String(4), nullable=False)
    fin: Mapped[str] = mapped_column(String(4), nullable=False)
    transferencia: Mapped[int] = mapped_column(TINYINT(1), nullable=False)
    entrada_vigor: Mapped[str] = mapped_column(String(4), nullable=False)
    id_categoria: Mapped[str] = mapped_column(String(8), nullable=False)
    nomina: Mapped[int] = mapped_column(TINYINT(1), nullable=False)


class IUnidadExcelencia(Base):
    __tablename__ = "i_unidad_excelencia"
    __table_args__ = {"schema": "prisma"}

    idUdExcelencia: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(
        VARCHAR(500, charset="utf8mb4", collation="utf8mb4_spanish_ci"), nullable=False
    )
    resumen: Mapped[str] = mapped_column(Text, nullable=False)
    fecha_creacion: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    acronimo: Mapped[str | None] = mapped_column(
        VARCHAR(20, charset="utf8mb4", collation="utf8mb4_spanish_ci"),
        comment="Nombre acortado",
    )
    ambito: Mapped[str | None] = mapped_column(String(100))
    url: Mapped[str | None] = mapped_column(Text)


class IUnidadExcelenciaLineaInvestigacion(Base):
    __tablename__ = "i_unidad_excelencia_linea_investigacion"
    __table_args__ = {"schema": "prisma"}

    idUdExcelencia: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idLineaInvestigacion: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class IUnidadExcelenciaPalabraClave(Base):
    __tablename__ = "i_unidad_excelencia_palabra_clave"
    __table_args__ = {"schema": "prisma"}

    idUdExcelencia: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    idPalabraClave: Mapped[int] = mapped_column(BIGINT(20), primary_key=True)
    forzar_visible: Mapped[int] = mapped_column(
        TINYINT(1), nullable=False, server_default=text("0")
    )
    fecha: Mapped[datetime.date | None] = mapped_column(Date)


class Institucion(Base):
    __tablename__ = "institucion"
    __table_args__ = (
        Index("id_ror", "id_ror", unique=True),
        {"schema": "prisma"},
    )

    idInstitucion: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True
    )
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    id_ror: Mapped[str | None] = mapped_column(String(9))
    tipo_organizacion: Mapped[str | None] = mapped_column(
        VARCHAR(50, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    enlace: Mapped[str | None] = mapped_column(
        VARCHAR(250, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    acronimos: Mapped[str | None] = mapped_column(
        VARCHAR(100, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    coordenadas_lat: Mapped[str | None] = mapped_column(
        "coordenadas.lat",
        VARCHAR(40, charset="utf8mb3", collation="utf8mb3_general_ci"),
    )
    coordenadas_lng: Mapped[str | None] = mapped_column(
        "coordenadas.lng",
        VARCHAR(40, charset="utf8mb3", collation="utf8mb3_general_ci"),
    )
    ciudad: Mapped[str | None] = mapped_column(
        VARCHAR(75, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    pais: Mapped[str | None] = mapped_column(
        VARCHAR(75, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
    fundref_preferido: Mapped[str | None] = mapped_column(
        VARCHAR(75, charset="utf8mb3", collation="utf8mb3_general_ci")
    )
