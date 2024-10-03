from models.attribute import Attribute
from models.model import Model


class Departamento(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_departamento",
        alias="departamento",
        primary_key="idDepartamento",
    ):
        attributes = [
            Attribute(column_name="idDepartamento"),
            Attribute(column_name="nombre"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )
