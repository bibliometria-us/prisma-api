from typing import List
from models.attribute import Attribute
from models.model import Model, Component
from models.institucion import Institucion


class Colectivo(Model):

    def __init__(
        self,
        table_name,
        alias,
        primary_key,
        tabla_relacion_investigador,
        db_name="prisma",
        instituciones=Institucion(),
    ):
        self.instituciones = instituciones
        self.tabla_relacion_investigador = tabla_relacion_investigador
        attributes = [
            Attribute(column_name=primary_key),
            Attribute(column_name="nombre"),
            Attribute(column_name="acronimo"),
            Attribute(column_name="ambito"),
        ]
        components = [
            Component(
                List[Institucion],
                "instituciones",
                "get_instituciones",
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

    def get_colectivo_from_investigador(self, idInvestigador: int) -> None:
        query = f"""SELECT {self.metadata.primary_key} FROM {self.metadata.db_name}.{self.tabla_relacion_investigador}
                WHERE idInvestigador = %(idInvestigador)s"""

        params = {"idInvestigador": idInvestigador}

        result = self.db.ejecutarConsulta(query, params)

        if self.db.has_rows():
            self.set_attribute(self.metadata.primary_key, result[1][0])
            self.get()

    def update_colectivo_from_investigador(
        self, idInvestigador: int, rol, actualizado=True
    ):
        query = f"""REPLACE INTO {self.metadata.db_name}.{self.tabla_relacion_investigador} ({self.metadata.primary_key}, idInvestigador, rol, actualizado)
                    VALUES (%(idColectivo)s, %(idInvestigador)s, %(rol)s, %(actualizado)s)"""

        params = {
            "idColectivo": self.get_primary_key().value,
            "idInvestigador": idInvestigador,
            "rol": rol,
            "actualizado": actualizado,
        }

        self.db.ejecutarConsulta(query, params)

    def delete_colectivo_from_investigador(self, idInvestigador: int):
        query = f"""DELETE FROM {self.metadata.db_name}.{self.tabla_relacion_investigador} 
                    WHERE idInvestigador = %(idInvestigador)s"""

        params = {"idInvestigador": idInvestigador}

        self.db.ejecutarConsulta(query, params)

    def get_instituciones(self) -> None:

        query = f"""SELECT idInstitucion FROM prisma.i_institucion_colectivo 
        WHERE idColectivo = %(idColectivo)s AND tipo = %(tipo)s"""

        params = {
            "idColectivo": self.get_primary_key().value,
            "tipo": self.metadata.alias,
        }

        result = self.db.ejecutarConsulta(query, params)

        self.instituciones = []

        if not self.db.has_rows:
            return None

        ids_instituciones = [row[0] for row in result[1:]]

        for id in ids_instituciones:
            institucion = Institucion()
            institucion.set_attribute("idInstitucion", id)
            institucion.get()
            self.instituciones.append(institucion)

    def add_institucion(self, idInstitucion: str):
        query = """INSERT INTO prisma.i_institucion_colectivo (idInstitucion, idColectivo, tipo)
                VALUES (%(idInstitucion)s, %(idColectivo)s, %(tipo)s )"""

        params = {
            "idInstitucion": idInstitucion,
            "idColectivo": self.get_primary_key().value,
            "tipo": self.metadata.alias,
        }

        self.db.ejecutarConsulta(query, params)

    def delete_institucion(self, idInstitucion: str):
        query = """DELETE FROM prisma.i_institucion_colectivo
                WHERE idInstitucion = %(idInstitucion)s AND idColectivo = %(idColectivo)s AND tipo = %(tipo)s """

        params = {
            "idInstitucion": idInstitucion,
            "idColectivo": self.get_primary_key().value,
            "tipo": self.metadata.alias,
        }

        self.db.ejecutarConsulta(query, params)
