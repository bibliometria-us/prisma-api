from models.attribute import Attribute
from models.model import Model


class PalabraClave(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_palabra_clave",
        alias="palabra_clave",
        primary_key="idPalabraClave",
    ):
        attributes = [
            Attribute(column_name="idPalabraClave"),
            Attribute(column_name="nombre"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )
