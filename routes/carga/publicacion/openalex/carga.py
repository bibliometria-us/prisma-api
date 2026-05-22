from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.exception import ErrorCargaPublicacion
from routes.carga.publicacion.extraccion_publicacion import ExtraccionPublicacion
from routes.carga.publicacion.openalex.parser import OpenalexParser
from integration.apis.openalex.openalex import OpenalexAPI
import json


class ExtraccionPublicacionOpenalex(ExtraccionPublicacion):
    def __init__(
        self,
        db: BaseDatos = None,
        id_carga=None,
        auto_commit=True,
        autor=None,
        tipo_carga=None,
    ) -> None:

        super().__init__(db, id_carga, auto_commit, autor=autor, tipo_carga=tipo_carga)
        self.carga.origen = "Openalex"
        self.clase_api = OpenalexAPI
        self.clase_parser = OpenalexParser
        self.clase_extraccion = ExtraccionPublicacionOpenalex

    def carga_publicacion(self, tipo: str, id: str):
        funciones = {
            "openalex": self.cargar_publicacion_por_id,
            "doi": self.cargar_publicacion_por_doi,
        }
        funcion = funciones.get(tipo)
        if funcion:
            return funcion(id)
        else:
            raise ErrorCargaPublicacion(
                f"El identificador tipo {tipo} no está soportado."
            )

    def cargar_publicacion_por_id(self, id: str):
        api = OpenalexAPI()
        records = api.get_publicaciones_por_id(id=id)
        if len(records) == 0:
            return None
        for publicacion in records:
            parser = OpenalexParser(data=publicacion)
            self.datos = parser.datos_carga_publicacion
            self.carga.cargar_publicacion()

        return self.carga.id_publicacion

    def cargar_publicacion_por_doi(self, id: str):
        api = OpenalexAPI()
        records = api.get_publicaciones_por_doi(id=id)
        if len(records) == 0:
            return None
        for publicacion in records:
            parser = OpenalexParser(data=publicacion)
            self.datos = parser.datos_carga_publicacion
            self.carga.cargar_publicacion()

        return self.carga.id_publicacion

    def get_registros_por_investigador(self):
        pass
