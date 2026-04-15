from db.conexion import BaseDatos
from routes.carga.carga import Carga
from routes.carga.publicacion.datos_carga_publicacion import DatosCargaFuente
from routes.carga.registro_cambios import ProblemaCarga, RegistroCambios


class CargaFuente(Carga):
    """
    Clase que representa una carga de fuente
    """

    def __init__(
        self,
        db: BaseDatos = None,
        id_carga=None,
        auto_commit=True,
        autor=None,
        tipo_carga=None,
    ) -> None:
        self.id_carga = id_carga or RegistroCambios.generar_id_carga()
        self.datos: DatosCargaFuente = None
        self.datos_antiguos: DatosCargaFuente = None
        self.auto_commit = auto_commit
        self.start_database(db)
        self.id_publicacion = 0
        self.fuente_existente = False
        self.origen = None
        self.autor = autor
        self.tipo_carga = tipo_carga
        self.problemas_carga: list[ProblemaCarga] = []
        self.lista_registros: list[RegistroCambios] = []
