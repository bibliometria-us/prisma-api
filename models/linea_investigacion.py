from models.attribute import Attribute
from models.model import Model


class LineaInvestigacion(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_linea_investigacion",
        alias="linea_investigacion",
        primary_key="idLineaInvestigacion",
    ):
        attributes = [
            Attribute(column_name="idLineaInvestigacion"),
            Attribute(column_name="nombre"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )
