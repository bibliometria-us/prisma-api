from models.attribute import Attribute
from models.model import Model


class LineaInvestigacionDoctorado(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_linea_investigacion_doctorado",
        alias="linea_investigacion_doctorado",
        primary_key="idLineaInvestigacion",
    ):
        attributes = [
            Attribute(column_name="idLineaInvestigacion"),
            Attribute(column_name="idDoctorado"),
            Attribute(column_name="codigo"),
            Attribute(column_name="nombre"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )
