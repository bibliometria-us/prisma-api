from typing import List
from models.attribute import Attribute
from models.colectivo.colectivo import Colectivo
from models.institucion import Institucion
from models.linea_investigacion import LineaInvestigacion
from models.model import Component
from models.palabra_clave import PalabraClave
from utils.format import table_to_pandas


class Grupo(Colectivo):

    def __init__(
        self,
        table_name="i_grupo",
        alias="grupo",
        primary_key="idGrupo",
        tabla_intermedia="i_grupo_investigador",
        db_name="prisma",
        instituciones: List[Institucion] = [],
        palabras_clave: List[PalabraClave] = [],
        lineas_investigacion: List[LineaInvestigacion] = [],
    ):

        super().__init__(
            table_name,
            alias,
            primary_key,
            tabla_intermedia,
            db_name,
            instituciones,
        )
        self.attributes["rama"] = Attribute(column_name="rama")
        self.attributes["codigo"] = Attribute(column_name="codigo")
        self.attributes["institucion"] = Attribute(column_name="institucion")
        self.attributes["estado"] = Attribute(column_name="estado")

        self.components["palabras_clave"] = Component(
            type=PalabraClave,
            name="palabras_clave",
            getter="get_palabras_clave",
            target_table="i_palabra_clave",
            intermediate_table="i_grupo_palabra_clave",
            cardinality="many",
            enabled=True,
        )

        self.components["lineas_investigacion"] = Component(
            type=LineaInvestigacion,
            name="lineas_investigacion",
            target_table="i_linea_investigacion",
            intermediate_table="i_grupo_linea_investigacion",
            cardinality="many",
            enabled=True,
        )

        self.palabras_clave = palabras_clave
        self.lineas_investigacion = lineas_investigacion
