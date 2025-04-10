from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.idus.parser import IdusParser
from tests.cargas.publicacion.test_carga_publicacion import comparar_publicacion
from tests.integration.idus.test_idus import leer_fichero_json_idus


def test_carga_idus(database: BaseDatos):
    database.set_savepoint("carga_idus")
    try:
        carga_idus(database)
    finally:
        database.rollback_to_savepoint("carga_idus")


def carga_idus(database: BaseDatos):
    publicaciones = leer_fichero_json_idus()

    lista_datos_publicacion = []
    for publicacion in publicaciones:
        parser = IdusParser(data=publicacion)
        datos = parser.datos_carga_publicacion
        lista_datos_publicacion.append(datos)
        comparar_publicacion(database, datos)
