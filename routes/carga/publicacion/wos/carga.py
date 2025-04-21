from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.wos.parser import WosParser
from integration.apis.clarivate.wos.wos_api import WosAPI
from utils import format
import json


class CargaPublicacionWos(CargaPublicacion):
    def __init__(self, db: BaseDatos = None, id_carga=None, auto_commit=True) -> None:

        super().__init__(db, id_carga, auto_commit)
        self.origen = "WOS"

    def carga_publicacion(self, tipo: str, id: str):
        funciones = {
            "wos": self.cargar_publicacion_por_id,
            "doi": self.cargar_publicacion_por_doi,
        }
        funcion = funciones.get(tipo)
        if funcion:
            return funcion(id)
        else:
            raise ValueError(f"El tipo {tipo} no está soportado.")

    def cargar_publicacion_por_id(self, id: str):
        api = WosAPI()
        records = api.get_publicaciones_por_id(id=id)
        if len(records) == 0:
            return None

        parser = WosParser(data=records[0])
        self.datos = parser.datos_carga_publicacion
        self.cargar_publicacion()

        return self.id_publicacion

    def cargar_publicacion_por_doi(self, id: str):
        api = WosAPI()
        records = api.get_publicaciones_por_doi(id=id)
        if len(records) == 0:
            return None

        parser = WosParser(data=records[0])
        self.datos = parser.datos_carga_publicacion
        self.cargar_publicacion()

        return self.id_publicacion

    def cargar_publicaciones_por_investigador(
        self, id_investigador: str, agno_inicio: str = None, agno_fin: str = None
    ):
        api = WosAPI()
        records = api.get_publicaciones_por_investigador_fecha(id_inves=id_investigador)

        if len(records) == 0:
            raise ValueError(f"El id {id} no devuelve ningún resultado.")
        for publicacion in records:
            parser = WosParser(data=publicacion)
            self.datos = parser.datos_carga_publicacion
            self.cargar_publicacion()

        return None
