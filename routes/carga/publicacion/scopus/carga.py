from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.exception import ErrorCargaPublicacion
from routes.carga.publicacion.extraccion_publicacion import ExtraccionPublicacion
from routes.carga.publicacion.scopus.parser import ScopusParser
from integration.apis.elsevier.scopus_search.scopus_search import ScopusSearch
from utils import format
import json


class ExtraccionPublicacionScopus(ExtraccionPublicacion):
    def __init__(
        self,
        db: BaseDatos = None,
        id_carga=None,
        auto_commit=True,
        autor=None,
        tipo_carga=None,
    ) -> None:

        super().__init__(db, id_carga, auto_commit, autor=autor, tipo_carga=tipo_carga)
        self.carga.origen = "Scopus"
        self.clase_api = ScopusSearch
        self.clase_parser = ScopusParser
        self.clase_extraccion = ExtraccionPublicacionScopus

    def carga_publicacion(self, tipo: str, id: str):
        funciones = {
            "scopus": self.cargar_publicacion_por_id,
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
        api = ScopusSearch()
        records = api.get_publicaciones_por_id(id_pub=id)
        if len(records) == 0:
            return None
        for publicacion in records:
            parser = ScopusParser(data=publicacion)
            self.carga.datos = parser.datos_carga_publicacion
            self.carga.cargar_publicacion()

        return self.carga.id_publicacion

    def cargar_publicacion_por_doi(self, id: str):
        api = ScopusSearch()
        records = api.get_publicaciones_por_doi(id_pub=id)

        if len(records) == 0:
            # TODO: Devolver nulo y gestionarlo en el método de la API
            return None
        for publicacion in records:
            parser = ScopusParser(data=publicacion)
            self.carga.datos = parser.datos_carga_publicacion
            self.carga.cargar_publicacion()
        
        return self.carga.id_publicacion


    def get_registros_por_investigador(
        self, id_investigador: str, agno_inicio: str = None, agno_fin: str = None
    ):
        if not self.carga.db:
            self.carga.db = BaseDatos()

        api = ScopusSearch()
        try:
            records = api.get_publicaciones_por_investigador_fecha(
                id_inves=id_investigador, agno_inicio=agno_inicio, agno_fin=agno_fin
            )

            if len(records) == 0:
                raise ValueError(
                    f"El id {id_investigador} no devuelve ningún resultado."
                )

        except Exception as e:
            self.carga.errores.append(str(e))
            return
