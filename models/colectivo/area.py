from models.attribute import Attribute
from models.colectivo.rama import Rama
from models.model import Component, Model


class Area(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_area",
        alias="area",
        primary_key="idArea",
    ):
        attributes = [
            Attribute(column_name="idArea"),
            Attribute(column_name="nombre"),
            Attribute(column_name="idRama", visible=0),
        ]
        components = [
            Component(
                type=Rama,
                name="rama",
                target_table="i_rama",
                foreign_key="idRama",
                cardinality="single",
                enabled=True,
            ),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
            components=components,
        )
