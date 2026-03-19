from abc import ABC, abstractmethod

from config.publicacion.tipos_publicacion import mapear_tipo_publicacion
from routes.carga.publicacion.datos_carga_publicacion import DatosCargaPublicacion
from routes.carga.publicacion.exception import ErrorCargaPublicacion


class Parser(ABC):
    def __init__(self):
        self.datos_carga_publicacion = DatosCargaPublicacion()

    @abstractmethod
    def set_fuente_datos(self):
        pass

    def api_request(self):
        pass

    @abstractmethod
    def cargar_titulo(self):
        pass

    @abstractmethod
    def cargar_titulo_alternativo(self):
        pass

    @abstractmethod
    def origen_tipo(self) -> str:
        pass

    def cargar_tipo(self):
        tipo = self.origen_tipo()
        valor = mapear_tipo_publicacion(
            origen=self.datos_carga_publicacion.fuente_datos.lower(), nombre_origen=tipo
        )

        if not valor:
            raise ErrorCargaPublicacion(f"Tipo de publicación '{tipo}' no soportado.")

        self.datos_carga_publicacion.set_tipo(valor)

    @abstractmethod
    def cargar_autores(self):
        pass

    @abstractmethod
    def cargar_editores(self):
        pass

    @abstractmethod
    def cargar_año_publicacion(self):
        pass

    @abstractmethod
    def cargar_fecha_publicacion(self):
        pass

    @abstractmethod
    def cargar_identificadores(self):
        pass

    @abstractmethod
    def cargar_datos(self):
        pass

    @abstractmethod
    def cargar_fuente(self):
        pass

    @abstractmethod
    def cargar_financiacion(self):
        pass

    @abstractmethod
    def carga_acceso_abierto(self):
        pass

    def carga(self):
        self.set_fuente_datos()
        self.cargar_titulo()
        self.cargar_titulo_alternativo()
        self.cargar_tipo()
        self.cargar_autores()
        self.cargar_editores()
        self.cargar_año_publicacion()
        self.cargar_fecha_publicacion()
        self.cargar_identificadores()
        self.cargar_datos()
        self.cargar_financiacion()
        self.cargar_fuente()
        self.carga_acceso_abierto()
        self.datos_carga_publicacion.normalizar_fuente()
        self.datos_carga_publicacion.close()
