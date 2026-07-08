from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.crossref.parser import CrossrefParser
from integration.apis.crossref.crossref.crossref import CrossrefAPI
import json

from routes.carga.publicacion.exception import ErrorCargaPublicacion
from routes.carga.publicacion.extraccion_publicacion import ExtraccionPublicacion


class ExtraccionPublicacionCrossref(ExtraccionPublicacion):
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
        self.carga.origen = "Crossref"

    def carga_publicacion(self, tipo: str, id: str):
        funciones = {
            "doi": self.cargar_publicacion_por_doi,
            "crossref": self.cargar_publicacion_por_doi,
        }
        funcion = funciones.get(tipo)
        if funcion:
            return funcion(id)
        else:
            raise ErrorCargaPublicacion(
                f"El identificador tipo {tipo} no está soportado."
            )

    def cargar_publicacion_por_doi(self, id: str):
        api = CrossrefAPI()
        record = api.get_publicaciones_por_doi(id=id)
        if not record or len(record) == 0:
            return None

        parser = CrossrefParser(data=record)
        # Guardar los datos parseados
        self.carga.datos = parser.datos_carga_publicacion
        # Cargar la publicación en la base de datos
        self.carga.cargar_publicacion()

        return self.carga.id_publicacion

    def cargar_publicaciones_por_investigador_limitada_agnos(
        self, id_investigador: str, agno_inicio: str, agno_fin: str
    ):
        pass

    def get_registros_por_investigador(self):
        pass
