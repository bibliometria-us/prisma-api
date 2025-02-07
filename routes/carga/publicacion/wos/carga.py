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
            "wos_id": self.cargar_publicacion_por_id,
            "doi": self.cargar_publicacion_por_doi,
        }
        funcion = funciones.get(tipo)
        if funcion:
            funcion(id)
        else:
            raise ValueError(f"El tipo {tipo} no está soportado.")

    def cargar_publicacion_por_id(self, id: str):
        api = WosAPI()
        # TODO: restaurar
        # records = api.get_from_id(id=id)

        # TODO: borrar
        # -------------------------
        records = None
        # Abrir el archivo .txt que contiene el JSON
        with open("tests/cargas/generico/json_wos_1_pub.txt", "r") as file:
            # Cargar el contenido del archivo y convertirlo a un objeto Python
            records = json.load(file)
            search_results: dict = records.get("Data", {})
            records = search_results.get("Records", []).get("records", [])
        # ---------------------------
        if len(records) == 0:
            raise ValueError(f"El id {id} no devuelve ningún resultado.")
        for publicacion in records.get("REC", []):
            parser = WosParser(data=publicacion)
            self.datos = parser.datos_carga_publicacion
            self.cargar_publicacion()

        return None

    def cargar_publicacion_por_doi(self, id: str):
        api = WosAPI()
        # TODO: restaurar
        # records = api.get_from_doi(id=id)

        # TODO: borrar
        # -------------------------
        records = None
        # Abrir el archivo .txt que contiene el JSON
        with open("tests/cargas/generico/json_wos_1_pub.txt", "r") as file:
            # Cargar el contenido del archivo y convertirlo a un objeto Python
            records = json.load(file)
            search_results: dict = records.get("search-results", {})
            records = search_results.get("entry", [])
        # ---------------------------
        if len(records) == 0:
            raise ValueError(f"El id {id} no devuelve ningún resultado.")
        for publicacion in records:
            parser = WosParser(data=publicacion)
            self.datos = parser.datos_carga_publicacion
            self.cargar_publicacion()

        return None

    def cargar_publicaciones_por_investigador(id_investigador: str):
        pass

    def cargar_publicaciones_por_investigador_limitada_agnos(
        self, id_investigador: str, agno_inicio: str, agno_fin: str
    ):
        pass
