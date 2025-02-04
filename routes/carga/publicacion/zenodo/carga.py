from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.zenodo.parser import ZenodoParser
from integration.apis.zenodo.zenodo import ZenodoAPI


class CargaPublicacionZenodo(CargaPublicacion):
    def __init__(self, db: BaseDatos = None, id_carga=None, auto_commit=True) -> None:

        super().__init__(db, id_carga, auto_commit)
        self.origen = "Zenodo"

    def carga_publicacion(self, tipo: str, id: str):
        funciones = {
            "zenodo_id": self.cargar_publicacion_por_id,
            "doi": self.cargar_publicacion_por_doi,
        }
        funcion = funciones.get(tipo)
        if funcion:
            funcion(id)

    def cargar_publicacion_por_id(self, id: str):
        api = ZenodoAPI()
        records = api.get_from_id(id=id)

        if len(records) == 0:
            raise ValueError(f"El id {id} no devuelve ningún resultado.")
        for publicacion in records:
            # TODO: parser = ScopusParser(data=publicacion)
            # TODO: self.datos = parser.datos_carga_publicacion
            # TODO: self.cargar_publicacion()
            pass
        return None

    def cargar_publicacion_por_doi(self, id: str):
        api = ZenodoAPI()
        records = api.get_from_doi(id=id)

        if len(records) == 0:
            raise ValueError(f"El id {id} no devuelve ningún resultado.")
        for publicacion in records:
            # TODO: parser = ScopusParser(data=publicacion)
            # TODO: self.datos = parser.datos_carga_publicacion
            # TODO: self.cargar_publicacion()
            pass
        return None

    def cargar_publicaciones_por_investigador(id_investigador: str):
        pass

    def cargar_publicaciones_por_investigador_limitada_agnos(
        self, id_investigador: str, agno_inicio: str, agno_fin: str
    ):
        pass
