from typing import List
from models.attribute import Attribute
from models.model import Component
from models.colectivo.colectivo import Colectivo
from models.institucion import Institucion


class UnidadExcelencia(Colectivo):
    def __init__(
        self,
        table_name="i_unidad_excelencia",
        alias="unidad_excelencia",
        primary_key="idUdExcelencia",
        tabla_intermedia="i_miembro_unidad_excelencia",
        db_name="prisma",
        instituciones: List[Institucion] = [],
    ):
        super().__init__(
            table_name, alias, primary_key, tabla_intermedia, db_name, instituciones
        )
