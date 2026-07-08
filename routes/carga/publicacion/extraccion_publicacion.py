from abc import ABC, abstractmethod

from db.conexion import BaseDatos
from integration.apis.api import API
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.exception import ErrorCargaPublicacion
from routes.carga.publicacion.parser import Parser


class ExtraccionPublicacion(ABC):
    def __init__(
        self, db, id_carga, auto_commit, autor, tipo_carga, id_investigador=None
    ):
        super().__init__()
        self.carga = CargaPublicacion(
            db=db,
            id_carga=id_carga,
            auto_commit=auto_commit,
            autor=autor,
            tipo_carga=tipo_carga,
            id_investigador=id_investigador,
        )
        self.clase_api: API.__class__ = None
        self.clase_parser: Parser.__class__ = None
        self.clase_extraccion: ExtraccionPublicacion.__class__ = None

    @abstractmethod
    def carga_publicacion():
        pass

    def cargar_publicaciones_por_investigador(
        self,
        identificador_origen: str,
        agno_inicio: str = None,
        agno_fin: str = None,
        id_investigador: str = None,
    ):
        if not self.carga.db:
            self.carga.db = BaseDatos()

        api = self.clase_api()
        try:
            records = api.get_publicaciones_por_investigador_fecha(
                id_inves=identificador_origen,
                agno_inicio=agno_inicio,
                agno_fin=agno_fin,
            )

            if len(records) == 0:
                raise ValueError(
                    f"El id {identificador_origen} no devuelve ningún resultado."
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
                    id_investigador=int(id_investigador) if id_investigador else None,
                )
                extraccion.carga.datos = parser.datos_carga_publicacion
                extraccion.carga.cargar_publicacion()
            except ErrorCargaPublicacion:
                continue
            except Exception as e:
                self.carga.errores.append(str(e))

        return None
