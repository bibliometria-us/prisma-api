from abc import ABC, abstractmethod

from db.conexion import BaseDatos
from routes.carga.publicacion.datos_carga_publicacion import DatosCarga
from routes.carga.publicacion.exception import ErrorCargaPublicacion
from routes.carga.registro_cambios import ProblemaCarga, RegistroCambios


class Carga(ABC):
    @abstractmethod
    def __init__(
        self,
        db: BaseDatos = None,
        id_carga=None,
        auto_commit=True,
        autor=None,
        tipo_carga=None,
    ):
        self.id_carga = id_carga
        self.datos: DatosCarga = None
        self.datos_antiguos: DatosCarga = None
        self.auto_commit = auto_commit
        self.start_database(db)
        self.origen = None
        self.autor = autor
        self.tipo_carga = tipo_carga
        self.tipos_carga_validos = []
        self.problemas_carga: list[ProblemaCarga] = []
        self.lista_registros: list[RegistroCambios] = []

    def comprobar_tipo_carga(self):
        if self.tipo_carga not in self.tipos_carga_validos:
            raise ErrorCargaPublicacion(
                f"Tipo de carga no válido. Tipos válidos: {self.tipos_carga_validos}"
            )

    def busqueda(func):
        def wrapper(self: "Carga", *args, **kwargs):
            if self.datos_antiguos is not None:
                return func(self, *args, **kwargs)
            else:
                return None

        return wrapper

    def start_database(self, db: BaseDatos):
        """
        Crea la conexión con la base de datos
        """
        if db:
            self.db = db
            assert self.db.autocommit == False
        else:
            self.db = BaseDatos(
                database=None, autocommit=False, keep_connection_alive=True
            )
            self.db.startConnection()
            self.db.connection.start_transaction()

    def stop_database(self):
        """
        Crea la cierra y revierte cambios en la conexión con la base de datos
        """
        self.db.rollback()
        self.db.closeConnection()

    def commit_database(self):
        """
        Crea la cierra y persiste cambios en la conexión con la base de datos
        """
        if self.auto_commit:
            self.db.commit()
            self.db.closeConnection()

    def close_database(self):
        """
        Cierra la conexión con la base de datos
        """
        self.db.closeConnection()

    def insertar_registros(self):
        self.procesar_registros()
        for registro in self.lista_registros:
            registro.insertar(id_carga=self.id_carga)

    def insertar_problemas(self):
        for problema in self.problemas_carga:
            problema.insertar(id_carga=self.id_carga)

    @abstractmethod
    def procesar_registros(self):
        pass

    @abstractmethod
    def limpiar_registros_importacion(self):
        pass
