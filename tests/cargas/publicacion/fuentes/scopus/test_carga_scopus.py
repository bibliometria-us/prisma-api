import random
from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.scopus.parser import ScopusParser
from tests.cargas.publicacion.test_carga_publicacion import comparar_publicacion
from tests.integration.scopus.test_scopus import leer_fichero_json_scopus


def test_carga_scopus(database: BaseDatos):
    database.set_savepoint("carga_scopus")
    try:
        carga_scopus(database)
    finally:
        database.rollback_to_savepoint("carga_scopus")


def carga_scopus(database: BaseDatos, limit=None):
    publicaciones = leer_fichero_json_scopus()

    if limit:
        publicaciones = random.sample(publicaciones, min(limit, len(publicaciones)))

    lista_datos_publicacion = []
    for publicacion in publicaciones:
        parser = ScopusParser(data=publicacion)
        datos = parser.datos_carga_publicacion
        lista_datos_publicacion.append(datos)
        comparar_publicacion(database, datos, origen="scopus")
