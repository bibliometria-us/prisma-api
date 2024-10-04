from typing import List
from db.conexion import BaseDatos
from models.attribute import Attribute
from models.linea_investigacion import LineaInvestigacion
from models.model import Component
from models.colectivo.colectivo import Colectivo
from models.institucion import Institucion
from models.palabra_clave import PalabraClave


class Instituto(Colectivo):
    def __init__(
        self,
        table_name="i_instituto",
        alias="instituto",
        primary_key="idInstituto",
        tabla_intermedia="i_miembro_instituto",
        db_name="prisma",
        instituciones: List[Institucion] = [],
        palabras_clave: List[PalabraClave] = [],
        lineas_investigacion: List[LineaInvestigacion] = [],
        db: BaseDatos = None,
    ):

        super().__init__(
            table_name, alias, primary_key, tabla_intermedia, db_name, instituciones
        )
        self.components["palabras_clave"] = Component(
            type=PalabraClave,
            db_name="prisma",
            name="palabras_clave",
            getter="get_palabras_clave",
            target_table="i_palabra_clave",
            intermediate_table="i_instituto_palabra_clave",
            cardinality="many",
            enabled=True,
        )

        self.components["lineas_investigacion"] = Component(
            type=LineaInvestigacion,
            db_name="prisma",
            name="lineas_investigacion",
            getter="get_lineas_investigacion",
            target_table="i_palabra_clave",
            intermediate_table="i_instituto_linea_investigacion",
            cardinality="many",
            enabled=True,
        )

        self.palabras_clave = palabras_clave
        self.lineas_investigacion = lineas_investigacion
