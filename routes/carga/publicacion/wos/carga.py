from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.exception import ErrorCargaPublicacion
from routes.carga.publicacion.extraccion_publicacion import ExtraccionPublicacion
from routes.carga.publicacion.wos.parser import WosParser
from integration.apis.clarivate.wos.wos_api import WosAPI
from utils import format
import json


class ExtraccionPublicacionWos(ExtraccionPublicacion):
    def __init__(
        self,
        db: BaseDatos = None,
        id_carga=None,
        auto_commit=True,
        autor=None,
        tipo_carga=None,
        id_investigador=None,
    ) -> None:

        super().__init__(
            db,
            id_carga,
            auto_commit,
            autor=autor,
            tipo_carga=tipo_carga,
            id_investigador=id_investigador,
        )
        self.carga.origen = "WOS"
        self.clase_api = WosAPI
        self.clase_parser = WosParser
        self.clase_extraccion = ExtraccionPublicacionWos

    def carga_publicacion(self, tipo: str, id: str):
        funciones = {
            "wos": self.cargar_publicacion_por_id,
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
        api = WosAPI()
        records = api.get_publicaciones_por_id(id=id)
        if len(records) == 0:
            return None

        parser = WosParser(data=records[0])
        # Se limpia "records" por problema de contaminacion de variables.
        records.clear()
        self.carga.datos = parser.datos_carga_publicacion
        self.carga.cargar_publicacion()

        return self.carga.id_publicacion

    def cargar_publicacion_por_doi(self, id: str):
        api = WosAPI()
        records = api.get_publicaciones_por_doi(id=id)
        if len(records) == 0:
            return None

        parser = WosParser(data=records[0])
        self.carga.datos = parser.datos_carga_publicacion
        self.carga.cargar_publicacion()

        return self.carga.id_publicacion

    def get_registros_por_investigador(self):
        pass
