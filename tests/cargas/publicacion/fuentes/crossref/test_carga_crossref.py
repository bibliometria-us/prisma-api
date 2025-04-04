from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.crossref.parser import CrossrefParser
from tests.cargas.publicacion.test_carga_publicacion import comparar_publicacion
from tests.integration.crossref.test_crossref import leer_fichero_json_crossref


def test_carga_crossref(database: BaseDatos):
    database.set_savepoint("carga_crossref")
    try:
        carga_crossref(database)
    finally:
        database.rollback_to_savepoint("carga_crossref")


def carga_crossref(database: BaseDatos):
    publicaciones = leer_fichero_json_crossref()

    lista_datos_publicacion = []
    for publicacion in publicaciones:
        parser = CrossrefParser(data=publicacion)
        datos = parser.datos_carga_publicacion
        lista_datos_publicacion.append(datos)
        comparar_publicacion(database, datos)
