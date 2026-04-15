from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.wos.parser import WosParser
from tests.cargas.publicacion.test_carga_publicacion import comparar_publicacion
from tests.integration.wos.test_wos import leer_fichero_json_wos
import random


def test_carga_wos(database: BaseDatos):
    database.set_savepoint("carga_wos")
    try:
        carga_wos(database)
    finally:
        database.rollback_to_savepoint("carga_wos")


def carga_wos(database: BaseDatos, limit=None):
    publicaciones = leer_fichero_json_wos()

    if limit:
        publicaciones = random.sample(publicaciones, min(limit, len(publicaciones)))

    lista_datos_publicacion = []
    for publicacion in publicaciones:
        parser = WosParser(data=publicacion)
        datos = parser.datos_carga_publicacion
        lista_datos_publicacion.append(datos)
        comparar_publicacion(database, datos, origen="wos")
