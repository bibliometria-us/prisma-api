from models.attribute import Attribute
from models.doctorado.linea_investigacion_doctorado import LineaInvestigacionDoctorado
from models.model import Model, Component


class ProgramaDoctorado(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_doctorado",
        alias="programa_doctorado",
        primary_key="idDoctorado",
    ):
        attributes = [
            Attribute(column_name="idDoctorado"),
            Attribute(column_name="nombre"),
        ]

        components = [
            Component(
                type=LineaInvestigacionDoctorado,
                db_name="prisma",
                name="linea_investigacion_doctorado",
                target_table="i_linea_investigacion_doctorado",
                intermediate_table="i_doctorado_linea_investigacion",
                cardinality="many",
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

    def get_lineas_investigacion(self):
        pass
