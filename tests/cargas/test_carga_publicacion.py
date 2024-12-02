from pandas import DataFrame
from db.conexion import BaseDatos
from models.investigador import Investigador
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.datos_carga_publicacion import DatosCargaPublicacion
from tests.cargas.fuente import publicacion
from tests.seed.generator import crear_datos_investigador
from tests.utils.random_utils import random_element


# database = BaseDatos(test=True, autocommit=False, keep_connection_alive=True)


def test_carga_publicacion(database: BaseDatos, seed):

    source = publicacion
    datos_carga = DatosCargaPublicacion()
    datos_carga.from_dict(source=source)

    carga = CargaPublicacion(db=database)
    carga.datos = datos_carga
    carga.origen = "idUS"

    check_insertar_publicacion(carga)
    insertar_identificadores_investigador(carga)
    check_insertar_autores(carga)
    check_insertar_identificadores_publicacion(carga)
    check_insertar_datos_publicacion(carga)
    check_insertar_fuente(carga)

    check_insertar_problemas(carga)
    database.rollback_to_savepoint("seed")


def check_insertar_publicacion(carga: CargaPublicacion):
    carga.insertar_publicacion()
    query = "SELECT * FROM prisma.p_publicacion WHERE idPublicacion = %(idPublicacion)s"
    params = {
        "idPublicacion": carga.id_publicacion,
    }

    carga.db.ejecutarConsulta(query, params)
    df = carga.db.get_dataframe()
    row = df.iloc[0]

    assert row["agno"] == carga.datos.año_publicacion
    assert row["titulo"] == carga.datos.titulo
    assert row["tipo"] == carga.datos.tipo


def insertar_identificadores_investigador(carga: CargaPublicacion):
    query = """
            INSERT INTO prisma.i_identificador_investigador (idInvestigador, tipo, valor)
            VALUES (%(idInvestigador)s,%(tipo)s,%(valor)s)
            """

    for index, autor in enumerate(carga.datos.autores):
        for identificador in autor.ids:
            params = {
                "idInvestigador": index + 1,
                "tipo": identificador.tipo,
                "valor": identificador.valor,
            }
            carga.db.ejecutarConsulta(query, params)


def check_insertar_autores(carga: CargaPublicacion):
    carga.insertar_autores()
    query = "SELECT * FROM prisma.p_autor WHERE idPublicacion = %(idPublicacion)s"
    params = {
        "idPublicacion": carga.id_publicacion,
    }

    carga.db.ejecutarConsulta(query, params)
    df = carga.db.get_dataframe()

    for i, autor in enumerate(carga.datos.autores):
        assert carga.id_publicacion == df.iloc[i]["idPublicacion"]
        assert carga.id_publicacion
        assert autor.firma == df.iloc[i]["firma"]
        assert autor.tipo == df.iloc[i]["rol"]
        assert autor.orden == df.iloc[i]["orden"]
        assert autor.contacto == df.iloc[i]["contacto"]
        assert i + 1 == df.iloc[i]["idInvestigador"]


def check_insertar_identificadores_publicacion(carga: CargaPublicacion):
    carga.insertar_identificadores_publicacion()
    query = "SELECT * FROM prisma.p_identificador_publicacion WHERE idPublicacion = %(idPublicacion)s"
    params = {
        "idPublicacion": carga.id_publicacion,
    }

    carga.db.ejecutarConsulta(query, params)
    df = carga.db.get_dataframe()

    for i, identificador in enumerate(carga.datos.identificadores):
        assert carga.id_publicacion == df.iloc[i]["idPublicacion"]
        assert carga.id_publicacion
        assert identificador.tipo == df.iloc[i]["tipo"]
        assert identificador.valor == df.iloc[i]["valor"]


def check_insertar_datos_publicacion(carga: CargaPublicacion, origen_esperado=None):
    if not origen_esperado:
        origen_esperado = carga.origen

    carga.insertar_datos_publicacion()
    query = "SELECT * FROM prisma.p_dato_publicacion WHERE idPublicacion = %(idPublicacion)s"
    params = {
        "idPublicacion": carga.id_publicacion,
    }

    carga.db.ejecutarConsulta(query, params)
    df = carga.db.get_dataframe()

    for i, identificador in enumerate(carga.datos.datos):
        assert carga.id_publicacion == df.iloc[i]["idPublicacion"]
        assert carga.id_publicacion
        assert identificador.tipo == df.iloc[i]["tipo"]
        assert identificador.valor == df.iloc[i]["valor"]
        assert origen_esperado == df.iloc[i]["origen"]


def check_insertar_fuente(carga: CargaPublicacion):
    carga.insertar_fuente()
    query = "SELECT * FROM prisma.p_fuente WHERE idFuente = %(idFuente)s"
    params = {
        "idFuente": carga.datos.fuente.id_fuente,
    }

    carga.db.ejecutarConsulta(query, params)
    df = carga.db.get_dataframe()

    row = df.iloc[0]

    assert row["idFuente"] == carga.datos.fuente.id_fuente
    assert row["tipo"] == carga.datos.fuente.tipo
    assert row["titulo"] == carga.datos.fuente.titulo


def check_insertar_problemas(carga: CargaPublicacion):
    carga.origen = "Scopus"

    carga.datos.tipo = "Tipo erroneo"
    carga.datos.año_publicacion = "1900"
    carga.insertar_publicacion()

    carga.datos.datos[0].valor = "error"
    carga.insertar_datos_publicacion()

    carga.datos.identificadores[0].valor = "error"
    carga.insertar_identificadores_publicacion()

    carga.datos.autores[0].firma = "prueba"
    carga.insertar_autores()

    carga.datos.tipo = "Artículo"
    carga.datos.año_publicacion = "2014"
    carga.origen = "idUS"
    carga.datos.datos[0].valor = "743"
    carga.datos.identificadores[0].valor = "10.1016/j.nima.2013.12.056"

    carga.insertar_problemas()
    check_insertar_problemas_publicacion(carga)
    check_insertar_problemas_datos_publicacion(carga)
    check_insertar_problemas_identificadores_publicacion(carga)


def check_insertar_problemas_publicacion(carga: CargaPublicacion):
    query = "SELECT * FROM prisma.a_problemas WHERE tipo_dato = %(tipo_dato)s AND id_dato = %(id_dato)s"
    params = {
        "tipo_dato": "Publicación",
        "id_dato": carga.id_publicacion,
    }

    carga.db.ejecutarConsulta(query, params)
    df = carga.db.get_dataframe()

    assert df.iloc[0]["antigua_fuente"] == "idUS"
    assert df.iloc[0]["antiguo_valor"] == carga.datos.tipo
    assert df.iloc[0]["nueva_fuente"] == "Scopus"
    assert df.iloc[0]["nuevo_valor"] == "Tipo erroneo"

    assert df.iloc[1]["antigua_fuente"] == "idUS"
    assert df.iloc[1]["antiguo_valor"] == carga.datos.año_publicacion
    assert df.iloc[1]["nueva_fuente"] == "Scopus"
    assert df.iloc[1]["nuevo_valor"] == "1900"


def check_insertar_problemas_datos_publicacion(carga: CargaPublicacion):

    query = "SELECT * FROM prisma.a_problemas WHERE tipo_dato = %(tipo_dato)s AND id_dato = %(id_dato)s AND tipo_dato_2 = %(tipo_dato_2)s"
    params = {
        "tipo_dato": "Publicación",
        "id_dato": carga.id_publicacion,
        "tipo_dato_2": "Dato",
    }

    carga.db.ejecutarConsulta(query, params)
    df = carga.db.get_dataframe()

    assert df.iloc[0]["antigua_fuente"] == "idUS"
    assert df.iloc[0]["antiguo_valor"] == carga.datos.datos[0].valor
    assert df.iloc[0]["nueva_fuente"] == "Scopus"
    assert df.iloc[0]["nuevo_valor"] == "error"


def check_insertar_problemas_identificadores_publicacion(carga: CargaPublicacion):

    query = "SELECT * FROM prisma.a_problemas WHERE tipo_dato = %(tipo_dato)s AND id_dato = %(id_dato)s AND tipo_dato_2 = %(tipo_dato_2)s"
    params = {
        "tipo_dato": "Publicación",
        "id_dato": carga.id_publicacion,
        "tipo_dato_2": "Identificador",
    }

    carga.db.ejecutarConsulta(query, params)
    df = carga.db.get_dataframe()

    assert df.iloc[0]["antigua_fuente"] == "idUS"
    assert df.iloc[0]["antiguo_valor"] == carga.datos.identificadores[0].valor
    assert df.iloc[0]["nueva_fuente"] == "Scopus"
    assert df.iloc[0]["nuevo_valor"] == "error"
