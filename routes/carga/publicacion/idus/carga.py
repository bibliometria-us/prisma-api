from db.conexion import BaseDatos
from integration.apis.idus.idus import IdusAPI, IdusAPISearch
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.idus.parser import IdusParser


class CargaPublicacionIdus(CargaPublicacion):
    def __init__(self, db: BaseDatos = None, id_carga=None, auto_commit=True) -> None:

        super().__init__(db, id_carga, auto_commit)
        self.origen = "idUS"

    def cargar_publicacion_por_handle(self, handle):
        api = IdusAPISearch()
        record = api.get_from_handle(handle=handle)
        if len(api.result) == 0:
            return None
        parser = IdusParser(data=record)
        self.datos = parser.datos_carga_publicacion
        self.cargar_publicacion()

        return self.id_publicacion

    def cargar_publicacion_por_dict(self, data: dict):
        parser = IdusParser(data=data)
        self.datos = parser.datos_carga_publicacion
        self.cargar_publicacion()

    def cargar_publicaciones_por_handle(self, handles: list[str], batch=True):
        """Para cada handle, instanciamos un nuevo objeto de carga y le pasamos el handle"""
        for handle in handles:
            carga = CargaPublicacionIdus(
                db=self.db, id_carga=self.id_carga, auto_commit=False
            )
            carga.cargar_publicacion_por_handle(handle)
            if not batch:
                carga.commit_database()
        if batch:
            self.commit_database()

    def cargar_publicaciones_por_dict(self, data_list: list[dict], batch=True):
        """Para cada diccionario, instanciamos un nuevo objeto de carga y le pasamos los datos"""
        for data in data_list:
            carga = CargaPublicacionIdus(
                db=self.db, id_carga=self.id_carga, auto_commit=False
            )
            carga.cargar_publicacion_por_dict(data)
            if not batch:
                carga.commit_database()
        if batch:
            self.commit_database()
