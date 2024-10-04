from db.conexion import BaseDatos
from models.attribute import Attribute
from models.model import Component, Model


class Rama(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_rama",
        alias="rama",
        primary_key="idRama",
        db: BaseDatos = None,
    ):
        attributes = [
            Attribute(column_name="idRama"),
            Attribute(column_name="nombre"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
            db=db or BaseDatos(),
        )
