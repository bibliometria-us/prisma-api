from typing import List
from models.attribute import Attribute
from models.model import Component
from models.colectivo.colectivo import Colectivo
from models.institucion import Institucion


class Instituto(Colectivo):
    def __init__(
        self,
        table_name="i_instituto",
        alias="instituto",
        primary_key="idInstituto",
        tabla_intermedia="i_miembro_instituto",
        db_name="prisma",
        instituciones: List[Institucion] = [],
    ):
        super().__init__(
            table_name, alias, primary_key, tabla_intermedia, db_name, instituciones
        )
