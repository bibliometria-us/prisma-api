import random
from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.openalex.parser import OpenalexParser
from tests.cargas.publicacion.test_carga_publicacion import comparar_publicacion
from tests.integration.openalex.test_openalex import leer_fichero_json_openalex


def test_carga_openalex(database: BaseDatos):
    database.set_savepoint("carga_openalex")
    try:
        carga_openalex(database)
    finally:
        database.rollback_to_savepoint("carga_openalex")


def carga_openalex(database: BaseDatos, limit=None):
    publicaciones = leer_fichero_json_openalex()

    if limit:
        publicaciones = random.sample(publicaciones, min(limit, len(publicaciones)))

    lista_datos_publicacion = []
    for publicacion in publicaciones:
        parser = OpenalexParser(data=publicacion)
        datos = parser.datos_carga_publicacion
        lista_datos_publicacion.append(datos)
        comparar_publicacion(database, datos, origen="openalex")
