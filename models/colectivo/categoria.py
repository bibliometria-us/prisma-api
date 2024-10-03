from models.attribute import Attribute
from models.model import Model


class Categoria(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_categoria",
        alias="categoria",
        primary_key="idCategoria",
    ):
        attributes = [
            Attribute(column_name="idCategoria"),
            Attribute(column_name="nombre"),
            Attribute(column_name="femenino"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )
