from pandas import DataFrame
from db.conexion import BaseDatos
from models.investigador import Investigador
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.datos_carga_publicacion import DatosCargaPublicacion
from tests.cargas.fuente import publicacion
from tests.seed.generator import crear_datos_investigador


# database = BaseDatos(test=True, autocommit=False, keep_connection_alive=True)


def test_carga_publicacion(database: BaseDatos, seed: dict[str, dict]):

    source = publicacion
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

    assert datos_carga.año_publicacion == carga.datos_antiguos.año_publicacion
    assert datos_carga.titulo == carga.datos_antiguos.titulo
    assert datos_carga.tipo == carga.datos_antiguos.tipo
    assert datos_carga.autores == carga.datos_antiguos.autores
    assert datos_carga.identificadores == carga.datos_antiguos.identificadores
    assert datos_carga.datos == carga.datos_antiguos.datos
    assert datos_carga.fuente == carga.datos_antiguos.fuente
    assert datos_carga.financiacion == carga.datos_antiguos.financiacion
    assert datos_carga.acceso_abierto == carga.datos_antiguos.acceso_abierto
    assert datos_carga.fechas_publicacion == carga.datos_antiguos.fechas_publicacion
