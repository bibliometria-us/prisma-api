from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.openalex.parser import OpenalexParser
from integration.apis.openalex.openalex import OpenalexAPI
import json


class CargaPublicacionOpenalex(CargaPublicacion):
    def __init__(self, db: BaseDatos = None, id_carga=None, auto_commit=True) -> None:

        super().__init__(db, id_carga, auto_commit)
        self.origen = "Openalex"

    def carga_publicacion(self, tipo: str, id: str):
        funciones = {
            "openalex_id": self.cargar_publicacion_por_id,
            "doi": self.cargar_publicacion_por_doi,
        }
        funcion = funciones.get(tipo)
        if funcion:
            return funcion(id)
        else:
            raise ValueError(f"El tipo {tipo} no está soportado.")

    def cargar_publicacion_por_id(self, id: str):
        api = OpenalexAPI()
        records = api.get_publicaciones_por_id(id=id)
        if len(records) == 0:
            return None
        for publicacion in records:
            parser = OpenalexParser(data=publicacion)
            self.datos = parser.datos_carga_publicacion
            self.cargar_publicacion()

        return self.id_publicacion

    def cargar_publicacion_por_doi(self, id: str):
        api = OpenalexAPI()
        records = api.get_publicaciones_por_doi(id=id)
        if len(records) == 0:
            return None
        for publicacion in records:
            parser = OpenalexParser(data=publicacion)
            self.datos = parser.datos_carga_publicacion
            self.cargar_publicacion()

        return self.id_publicacion

    def cargar_publicaciones_por_investigador(
        id_investigador: str, agno_inicio: str = None, agno_fin: str = None
    ):
        pass
