from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.crossref.parser import CrossrefParser
from integration.apis.crossref.crossref.crossref import CrossrefAPI
import json


class CargaPublicacionCrossref(CargaPublicacion):
    def __init__(self, db: BaseDatos = None, id_carga=None, auto_commit=True) -> None:

        super().__init__(db, id_carga, auto_commit)
        self.origen = "Crossref"

    def carga_publicacion(self, tipo: str, id: str):
        funciones = {
            "doi": self.cargar_publicacion_por_doi,
        }
        funcion = funciones.get(tipo)
        if funcion:
            funcion(id)
        else:
            raise ValueError(f"El tipo {tipo} no está soportado.")

    def cargar_publicacion_por_doi(self, id: str):
        api = CrossrefAPI()
        # TODO: restaurar
        # records = api.get_from_doi(id=id)

        # TODO: borrar
        # -------------------------
        records = None
        # Abrir el archivo .txt que contiene el JSON
        with open("tests/cargas/generico/json_xref_1_pub.txt", "r") as file:
            # Cargar el contenido del archivo y convertirlo a un objeto Python
            records = json.load(file)
            # En Xref solo viene una publicación en la API
            record: dict = records.get("message", {})
        # ---------------------------
        # TODO: ver como funciona cuando viene de la API
        if len(records) == 0:
            raise ValueError(f"El id {id} no devuelve ningún resultado.")

        parser = CrossrefParser(data=record)
        self.datos = parser.datos_carga_publicacion
        self.cargar_publicacion()

        return None

    def cargar_publicaciones_por_investigador(id_investigador: str):
        pass

    def cargar_publicaciones_por_investigador_limitada_agnos(
        self, id_investigador: str, agno_inicio: str, agno_fin: str
    ):
        pass
