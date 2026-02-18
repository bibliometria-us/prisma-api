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
            "crossref": self.cargar_publicacion_por_doi,
        }
        funcion = funciones.get(tipo)
        if funcion:
            return funcion(id)
        else:
            raise ValueError(f"El tipo {tipo} no está soportado.")

    def cargar_publicacion_por_doi(self, id: str):
        api = CrossrefAPI()
        record = api.get_publicaciones_por_doi(id=id)
        if not record or len(record) == 0:
            return None

        parser = CrossrefParser(data=record)
        # Guardar los datos parseados
        self.datos = parser.datos_carga_publicacion
        # Cargar la publicación en la base de datos
        self.cargar_publicacion()

        return self.id_publicacion

    def cargar_publicaciones_por_investigador(id_investigador: str):
        pass

    def cargar_publicaciones_por_investigador_limitada_agnos(
        self, id_investigador: str, agno_inicio: str, agno_fin: str
    ):
        pass
