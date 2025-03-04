from pandas import DataFrame
from db.conexion import BaseDatos
from models.investigador import Investigador
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.datos_carga_publicacion import DatosCargaPublicacion
from tests.cargas.fuente import get_publicacion_fuente
from tests.seed.generator import crear_datos_investigador


# database = BaseDatos(test=True, autocommit=False, keep_connection_alive=True)


def test_carga_publicacion(database: BaseDatos, seed: dict[str, dict]):

    source = get_publicacion_fuente()
    datos_carga = DatosCargaPublicacion()
    datos_carga.from_dict(source=source)

    comparar_publicacion(database, datos_carga)

    database.rollback_to_savepoint("seed")

    pass


def comparar_publicacion(database: BaseDatos, datos_carga: DatosCargaPublicacion):
    carga = CargaPublicacion(db=database)
    carga.datos = datos_carga
    carga.origen = "idUS"

    carga.cargar_publicacion()

    carga.cargar_publicacion()

    assert datos_carga == carga.datos_antiguos
