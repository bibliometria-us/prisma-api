from typing import List
from models.attribute import Attribute
from models.colectivo.area import Area
from models.colectivo.categoria import Categoria
from models.colectivo.centro import Centro
from models.colectivo.departamento import Departamento
from models.colectivo.grupo import Grupo
from models.condition import Condition
from models.linea_investigacion import (
    LineaInvestigacion,
    get_lineas_investigacion as _get_lineas_investigacion,
    add_linea_investigacion as _add_linea_investigacion,
    remove_linea_investigacion as _remove_linea_investigacion,
)
from models.model import Component, Model
from models.colectivo.unidad_excelencia import UnidadExcelencia
from models.colectivo.centro_mixto import CentroMixto
from models.colectivo.instituto import Instituto
from models.palabra_clave import (
    PalabraClave,
    get_palabras_clave as _get_palabras_clave,
    add_palabra_clave as _add_palabra_clave,
    remove_palabra_clave as _remove_palabra_clave,
)
from utils.decode import http_arg_decode


class Investigador(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_investigador",
        alias="investigador",
        primary_key="idInvestigador",
        enabled_components: list[str] = list(),
    ):
        attributes = [
            Attribute(column_name="idInvestigador"),
            Attribute(column_name="nombre"),
            Attribute(column_name="apellidos"),
            Attribute(column_name="docuIden", visible=0),
            Attribute(column_name="email"),
            Attribute(column_name="idDepartamento", visible=0),
            Attribute(column_name="idCategoria", visible=0),
            Attribute(column_name="idArea", visible=0),
            Attribute(column_name="fechaContratacion"),
            Attribute(column_name="idCentro", visible=0),
            Attribute(column_name="nacionalidad"),
            Attribute(column_name="sexo"),
            Attribute(column_name="fechaNacimiento"),
            Attribute(column_name="fechaNombramiento"),
            Attribute(column_name="perfilPublico"),
            Attribute(column_name="resumen", pre_processors=[http_arg_decode]),
        ]
        components = [
            Component(
                type=Centro,
                name="centro",
                target_table="i_centro",
                foreign_key="idCentro",
                cardinality="single",
            ),
            Component(
                type=Departamento,
                name="departamento",
                target_table="i_departamento",
                foreign_key="idDepartamento",
                cardinality="single",
            ),
            Component(
                type=Categoria,
                name="categoria",
                target_table="i_categoria",
                foreign_key="idCategoria",
                cardinality="single",
            ),
            Component(
                type=Area,
                name="area",
                target_table="i_area",
                foreign_key="idArea",
                cardinality="single",
            ),
            Component(
                type=Grupo,
                name="grupo",
                # getter="get_grupo",
                target_table="i_grupo",
                intermediate_table="i_grupo_investigador",
                cardinality="single",
            ),
            Component(
                type=UnidadExcelencia,
                name="unidad_excelencia",
                getter="get_unidad_excelencia",
                target_table="i_unidad_excelencia",
                intermediate_table="i_miembro_unidad_excelencia",
                cardinality="single",
            ),
            Component(
                type=CentroMixto,
                name="centro_mixto",
                getter="get_centro_mixto",
                target_table="i_centro_mixto",
                intermediate_table="i_miembro_centro_mixto",
                cardinality="single",
            ),
            Component(
                type=Instituto,
                name="instituto",
                getter="get_instituto",
                target_table="i_instituto",
                intermediate_table="i_miembro_instituto",
                cardinality="single",
            ),
            Component(
                type=PalabraClave,
                name="palabras_clave",
                target_table="i_palabra_clave",
                intermediate_table="i_investigador_palabra_clave",
                cardinality="many",
            ),
            Component(
                type=LineaInvestigacion,
                name="lineas_investigacion",
                target_table="i_linea_investigacion",
                intermediate_table="i_investigador_linea_investigacion",
                cardinality="many",
            ),
            Component(
                type=IdentificadorInvestigador,
                name="identificador_investigador",
                target_table="i_identificador_investigador",
                foreign_key="idInvestigador",
                foreign_target_column="idInvestigador",
                cardinality="many",
            ),
        ]
        self.max_palabras_clave = 10

        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
            components=components,
            enabled_components=enabled_components,
        )

    # GRUPOS

    def get_grupo(self) -> Grupo:
        query = "SELECT MAX(idGrupo) FROM prisma.i_grupo_investigador WHERE idInvestigador = %(idInvestigador)s"

        params = {"idInvestigador": self.get_attribute_value("idInvestigador")}

        result = self.db.ejecutarConsulta(query, params)
        id_grupo = result[1][0]

        if id_grupo:
            self.grupo.set_attribute("idGrupo", id_grupo)
            self.grupo.get()

        return self.grupo

    def actualizar_grupo(self, id_grupo, rol) -> None:
        query = """REPLACE INTO prisma.i_grupo_investigador (idInvestigador, idGrupo, rol)
        VALUES (%(idInvestigador)s, %(idGrupo)s, %(rol)s)"""

        params = {
            "idInvestigador": self.get_attribute_value("idInvestigador"),
            "idGrupo": id_grupo,
            "rol": rol,
        }

        result = self.db.ejecutarConsulta(query, params)

        return None

    def eliminar_grupo(self) -> None:
        self.actualizar_grupo("0", "Miembro")

    # UNIDADES DE EXCELENCIA

    def get_unidad_excelencia(self) -> UnidadExcelencia:
        self.unidad_excelencia.get_colectivo_from_investigador(
            self.get_primary_key().value
        )

        return self.unidad_excelencia

    def update_unidad_excelencia(
        self, unidad_excelencia, rol, actualizado=True
    ) -> None:
        self.unidad_excelencia.set_attribute("idUdExcelencia", unidad_excelencia)
        self.unidad_excelencia.get()
        self.unidad_excelencia.update_colectivo_from_investigador(
            idInvestigador=self.get_primary_key().value,
            rol=rol,
            actualizado=actualizado,
        )

    def delete_unidad_excelencia(self) -> None:
        self.unidad_excelencia.delete_colectivo_from_investigador(
            self.get_primary_key().value
        )
        self.unidad_excelencia = UnidadExcelencia()

    # CENTROS MIXTOS

    def get_centro_mixto(self) -> None:
        self.centro_mixto.get_colectivo_from_investigador(self.get_primary_key().value)

        return self.centro_mixto

    def update_centro_mixto(self, centro_mixto, rol, actualizado=True) -> None:
        self.centro_mixto.set_attribute("idCentroMixto", centro_mixto)
        self.centro_mixto.get()
        self.centro_mixto.update_colectivo_from_investigador(
            idInvestigador=self.get_primary_key().value,
            rol=rol,
            actualizado=actualizado,
        )

    def delete_centro_mixto(self) -> None:
        self.centro_mixto.delete_colectivo_from_investigador(
            self.get_primary_key().value
        )
        self.centro_mixto = CentroMixto()

    # INSTITUTOS

    def get_instituto(self) -> None:
        self.instituto.get_colectivo_from_investigador(self.get_primary_key().value)

        return self.instituto

    def update_instituto(self, instituto, rol, actualizado=True) -> None:
        self.instituto.set_attribute("idInstituto", instituto)
        self.instituto.get()
        self.instituto.update_colectivo_from_investigador(
            idInvestigador=self.get_primary_key().value,
            rol=rol,
            actualizado=actualizado,
        )

    def delete_instituto(self) -> None:
        self.instituto.delete_colectivo_from_investigador(self.get_primary_key().value)
        self.instituto = Instituto()

    def get_palabras_clave(self):
        return _get_palabras_clave(self)

    def add_palabra_clave(self, id_palabra_clave=None, nombre_palabra_clave=None):
        return _add_palabra_clave(
            self,
            id_palabra_clave=id_palabra_clave,
            nombre_palabra_clave=nombre_palabra_clave,
        )

    def remove_palabra_clave(self, id_palabra_clave):
        _remove_palabra_clave(
            self,
            id_palabra_clave=id_palabra_clave,
        )

    def get_lineas_investigacion(self):
        return _get_lineas_investigacion(self)

    def add_linea_investigacion(
        self, id_linea_investigacion=None, nombre_linea_investigacion=None
    ):
        return _add_linea_investigacion(
            self,
            id_linea_investigacion=id_linea_investigacion,
            nombre_linea_investigacion=nombre_linea_investigacion,
        )

    def remove_linea_investigacion(self, id_linea_investigacion):
        _remove_linea_investigacion(
            self,
            id_linea_investigacion=id_linea_investigacion,
        )


class IdentificadorInvestigador(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_identificador_investigador",
        alias="identificador_investigador",
        primary_key="idIdentificador",
    ):
        attributes = [
            Attribute(column_name="idIdentificador", visible=0),
            Attribute(column_name="idInvestigador", visible=0),
            Attribute(column_name="tipo"),
            Attribute(column_name="valor"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )

    def get(
        self,
        conditions: List[Condition] = None,
        all: bool = False,
        logical_operator: str = "AND",
    ) -> None:
        return super().get(conditions, all, logical_operator)
