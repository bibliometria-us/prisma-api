from models.attribute import Attribute
from models.condition import Condition
from models.linea_investigacion import LineaInvestigacion
from models.model import Component, Model
from models.palabra_clave import PalabraClave
from utils.format import table_to_pandas


class Grupo(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_grupo",
        alias="grupo",
        primary_key="idGrupo",
        palabras_clave: list[PalabraClave] = [],
        lineas_investigacion: list[LineaInvestigacion] = [],
    ):
        attributes = [
            Attribute(column_name="idGrupo"),
            Attribute(column_name="nombre"),
            Attribute(column_name="acronimo"),
            Attribute(column_name="rama"),
            Attribute(column_name="codigo"),
            Attribute(column_name="institucion"),
            Attribute(column_name="estado"),
        ]

        components = [
            Component(
                PalabraClave,
                "palabras_clave",
                "get_palabras_clave",
                enabled=True,
            ),
            Component(
                LineaInvestigacion,
                "lineas_investigacion",
                "get_lineas_investigacion",
                enabled=True,
            ),
        ]
        self.palabras_clave = palabras_clave
        self.lineas_investigacion = lineas_investigacion
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
            components=components,
        )

    # PALABRAS CLAVE
    def get_palabras_clave(self):
        object = PalabraClave()
        db_name = object.metadata.db_name
        table_name = object.metadata.table_name
        alias = object.metadata.alias
        attribute_keys = object.attributes.keys()
        columns = list(f"{alias}.{attribute}" for attribute in attribute_keys)

        query = f"""SELECT {",".join(columns)} FROM {db_name}.{table_name} {alias}
                LEFT JOIN {db_name}.i_grupo_palabra_clave gpc ON gpc.idPalabraClave = {alias}.idPalabraClave
                LEFT JOIN {db_name}.i_grupo grupo ON grupo.idGrupo = gpc.idGrupo
                WHERE grupo.idGrupo = '{self.attributes["idGrupo"].value}'
                """

        table = table_to_pandas(self.db.ejecutarConsulta(query))

        for index, row in table.iterrows():
            palabra_clave = PalabraClave()
            palabra_clave.set_attributes(
                {
                    "idPalabraClave": row["idPalabraClave"],
                    "nombre": row["nombre"],
                }
            )
            self.palabras_clave.append(palabra_clave)

    def add_palabra_clave(self, id_palabra_clave, fecha):

        query = f"""INSERT INTO prisma.i_grupo_palabra_clave (idGrupo, idPalabraClave, fecha)
                    VALUES ('{self.get_attribute_value("idGrupo")}', %(idPalabraClave)s, %(fecha)s)"""

        params = {
            "idPalabraClave": id_palabra_clave,
            "fecha": fecha,
        }

        result = self.db.ejecutarConsulta(query, params)

        return None

    def remove_palabra_clave(self, id_palabra_clave):
        query = f"""DELETE FROM prisma.i_grupo_palabra_clave
                    WHERE idGrupo = '{self.get_attribute_value("idGrupo")}'
                        AND idPalabraClave = %(idPalabraClave)s      
                """

        params = {"idPalabraClave": id_palabra_clave}

        result = self.db.ejecutarConsulta(query, params)

        return None

    # LINEAS DE INVESTIGACION
    def get_lineas_investigacion(self):
        object = LineaInvestigacion()
        db_name = object.metadata.db_name
        table_name = object.metadata.table_name
        alias = object.metadata.alias
        attribute_keys = object.attributes.keys()
        columns = list(f"{alias}.{attribute}" for attribute in attribute_keys)

        query = f"""SELECT {",".join(columns)} FROM {db_name}.{table_name} {alias}
                LEFT JOIN {db_name}.i_grupo_linea_investigacion gli ON gli.idLineaInvestigacion = {alias}.idLineaInvestigacion
                LEFT JOIN {db_name}.i_grupo grupo ON grupo.idGrupo = gli.idGrupo
                WHERE grupo.idGrupo = '{self.attributes["idGrupo"].value}'
                """

        table = table_to_pandas(self.db.ejecutarConsulta(query))

        for index, row in table.iterrows():
            linea_investigacion = LineaInvestigacion()
            linea_investigacion.set_attributes(
                {
                    "idLineaInvestigacion": row["idLineaInvestigacion"],
                    "nombre": row["nombre"],
                }
            )
            self.palabras_clave.append(linea_investigacion)

    def add_linea_investigacion(self, id_linea_investigacion, fecha):

        query = f"""INSERT INTO prisma.i_grupo_linea_investigacion (idGrupo, idLineaInvestigacion, fecha)
                    VALUES ('{self.get_attribute_value("idGrupo")}', %(idLineaInvestigacion)s, %(fecha)s)"""

        params = {
            "idLineaInvestigacion": id_linea_investigacion,
            "fecha": fecha,
        }

        result = self.db.ejecutarConsulta(query, params)

        return None

    def remove_linea_investigacion(self, id_linea_investigacion):
        query = f"""DELETE FROM prisma.i_grupo_linea_investigacion
                    WHERE idGrupo = '{self.get_attribute_value("idGrupo")}'
                        AND idLineaInvestigacion = %(idLineaInvestigacion)s      
                """

        params = {"idLineaInvestigacion": id_linea_investigacion}

        result = self.db.ejecutarConsulta(query, params)

        return None
