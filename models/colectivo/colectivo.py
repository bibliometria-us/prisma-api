from typing import List
from models.attribute import Attribute
from models.colectivo.exceptions.exceptions import (
    LimitePalabrasClave,
    LineaInvestigacionDuplicada,
    PalabraClaveDuplicada,
)
from models.linea_investigacion import (
    LineaInvestigacion,
    get_lineas_investigacion as _get_lineas_investigacion,
    add_linea_investigacion as _add_linea_investigacion,
    remove_linea_investigacion as _remove_linea_investigacion,
)
from models.model import Model, Component
from models.institucion import Institucion
from models.palabra_clave import (
    PalabraClave,
    get_palabras_clave as _get_palabras_clave,
    add_palabra_clave as _add_palabra_clave,
    remove_palabra_clave as _remove_palabra_clave,
)
from utils.decode import http_arg_decode
from utils.format import table_to_pandas


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
            Attribute(column_name="resumen", pre_processors=[http_arg_decode]),
            Attribute(column_name="fecha_creacion"),
        ]
        components = [
            Component(
                type=Institucion,
                name="instituciones",
                getter="get_instituciones",
                target_table="institucion",
                intermediate_table="i_institucion_colectivo",
                cardinality="many",
                enabled=True,
            )
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
            components=components,
        )
        self.max_palabras_clave = 10
        self.max_resumen = 5000

    def recortar_resumen(self):
        ### Recortar el contenido del resumen para que nos supere el máximo

        resumen: str = self.attributes.get("resumen").value

        resumen = resumen.strip() if resumen else None

        # Contar la cantidad de veces que aparecen \n y añadirlas al máximo multiplicado por 4
        cantidad_saltos_linea = resumen.count("\n")
        max_resumen = self.max_resumen + cantidad_saltos_linea
        resultado_resumen = resumen[0:max_resumen]
        self.attributes.get("resumen").value = resultado_resumen

    def _add(self, attribute_filter=[], insert_type="") -> None:
        self.recortar_resumen()

        return super()._add(attribute_filter, insert_type)

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

    def get_palabras_clave(self):
        return _get_palabras_clave(self)

    def add_palabra_clave(self, id_palabra_clave=None, nombre_palabra_clave=None):
        return _add_palabra_clave(
            self,
            id_palabra_clave=id_palabra_clave,
            nombre_palabra_clave=nombre_palabra_clave,
        )

    def remove_palabra_clave(self, id_palabra_clave):
        _remove_palabra_clave(
            self,
            id_palabra_clave=id_palabra_clave,
        )

    def get_lineas_investigacion(self):
        return _get_lineas_investigacion(self)

    def add_linea_investigacion(
        self, id_linea_investigacion=None, nombre_linea_investigacion=None
    ):
        return _add_linea_investigacion(
            self,
            id_linea_investigacion=id_linea_investigacion,
            nombre_linea_investigacion=nombre_linea_investigacion,
        )

    def remove_linea_investigacion(self, id_linea_investigacion):
        _remove_linea_investigacion(
            self,
            id_linea_investigacion=id_linea_investigacion,
        )
