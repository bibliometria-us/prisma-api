from db.conexion import BaseDatos
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
        db: BaseDatos = None,
    ):
        attributes = [
            Attribute(column_name="idArea"),
            Attribute(column_name="nombre"),
        ]
        components = [
            Component(
                type=Rama,
                db_name="prisma",
                name="rama",
                target_table="i_rama",
                foreign_key="idRama",
                foreign_target_column="idRama",
                cardinality="single",
                nullable=False,
            ),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
            components=components,
            db=db or BaseDatos(),
        )
