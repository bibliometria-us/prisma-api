from models.attribute import Attribute
from models.model import Model


class Institucion(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="institucion",
        alias="institucion",
        primary_key="idInstitucion",
    ):
        attributes = [
            Attribute(column_name="idInstitucion"),
            Attribute(column_name="nombre"),
            Attribute(column_name="id_ror"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )
