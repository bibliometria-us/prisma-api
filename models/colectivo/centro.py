from models.attribute import Attribute
from models.model import Model


class Centro(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_centro",
        alias="centro",
        primary_key="idCentro",
    ):
        attributes = [
            Attribute(column_name="idCentro"),
            Attribute(column_name="nombre"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )
