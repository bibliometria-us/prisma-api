from abc import ABC, abstractmethod

from db.conexion import BaseDatos
from integration.apis.api import API
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.exception import ErrorCargaPublicacion
from routes.carga.publicacion.parser import Parser


class ExtraccionPublicacion(ABC):
    def __init__(self, db, id_carga, auto_commit, autor, tipo_carga):
        super().__init__()
        self.carga = CargaPublicacion(
            db=db,
            id_carga=id_carga,
            auto_commit=auto_commit,
            autor=autor,
            tipo_carga=tipo_carga,
        )
        self.clase_api: API.__class__ = None
        self.clase_parser: Parser.__class__ = None
        self.clase_extraccion: ExtraccionPublicacion.__class__ = None

    def cargar_publicaciones_por_investigador(
        self,
        id_investigador: str,
        agno_inicio: str = None,
        agno_fin: str = None,
    ):
        if not self.carga.db:
            self.carga.db = BaseDatos()

        api = self.clase_api()
        try:
            records = api.get_publicaciones_por_investigador_fecha(
                id_inves=id_investigador, agno_inicio=agno_inicio, agno_fin=agno_fin
            )

            if len(records) == 0:
                raise ValueError(
                    f"El id {id_investigador} no devuelve ningún resultado."
                )

        except Exception as e:
            self.carga.errores.append(f"{self.carga.origen}: {str(e)}")
            return

        for publicacion in records:
            try:
                parser = self.clase_parser(data=publicacion)
                extraccion = self.clase_extraccion(
                    db=self.carga.db,
                    id_carga=self.carga.id_carga,
                    auto_commit=self.carga.auto_commit,
                    autor=self.carga.autor,
                    tipo_carga=self.carga.tipo_carga,
                )
                extraccion.carga.datos = parser.datos_carga_publicacion
                extraccion.carga.cargar_publicacion()
            except ErrorCargaPublicacion:
                continue
            except Exception as e:
                self.carga.errores.append(str(e))

        return None
