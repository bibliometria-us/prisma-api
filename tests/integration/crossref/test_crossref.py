import json
import os
import pprint

import pytest
from db.conexion import BaseDatos
from integration.apis.crossref.crossref.crossref import CrossrefAPI
from routes.carga.publicacion.crossref.parser import CrossrefParser
import xml.etree.ElementTree as ET

from routes.carga.publicacion.datos_carga_publicacion import DatosCargaPublicacion
from tests.integration.utils.utils import estadisticas_datos_publicacion


ids_crossref = [
    # "10.12688/openreseurope.17554.2",  # Artículo
    # "10.3233/faia240928",  # Capítulo
    # "10.1007/978-3-030-61705-9_13",  # Ponencia
    # "2-s2.0-85ccxcx2216 ",  # No existe
    # "10.1007/978-3-031-87312-6_16",
    # "10.1007/978-3-031-87312-6",
    "10.1016/j.daach.2025.e00417",
]


def test_busqueda_por_id():
    api = CrossrefAPI()
    for id in ids_crossref:
        record = api.get_publicaciones_por_doi(id)
        assert not api.response.get("service-error")
        parser = CrossrefParser(data=record)
        json = parser.datos_carga_publicacion.to_json()


@pytest.mark.skipif(
    os.path.exists("tests/integration/test_crossref/json_masivo_crossref.json"),
    reason="JSON file already exists",
)
def test_masivo_guardado_json():
    api = CrossrefAPI()

    query = """SELECT valor FROM p_identificador_publicacion WHERE tipo = 'doi' ORDER BY RAND() LIMIT %(limit)s"""
    params = {"limit": 1000}

    bd = BaseDatos()
    bd.ejecutarConsulta(query, params)
    df = bd.get_dataframe()

    dois = df["valor"].tolist()
    records = []

    for doi in dois:
        record = api.get_publicaciones_por_doi(doi)
        if record:
            records.append(record)

    fuente = "crossref"
    FILENAME = f"tests/integration/{fuente}/json_masivo_{fuente}.json"

    # Cargar JSON de publicaciones (si existe)
    if os.path.exists(FILENAME):
        with open(FILENAME, "r") as file:
            existing_records = json.load(file)
    else:
        existing_records = []

    # Agregar nuevos registros a los existentes
    existing_records.extend(records)

    # Guardar cada registro en el archivo en cada iteración
    with open(FILENAME, "w") as file:
        json.dump(existing_records, file, indent=4)


def leer_fichero_json_crossref() -> list[DatosCargaPublicacion]:
    fuente = "crossref"
    FILENAME = f"tests/integration/{fuente}/json_masivo_{fuente}.json"

    # Cargar el archivo JSON
    with open(FILENAME, "r", encoding="utf-8") as file:
        publicaciones = json.load(file)  # Lista de diccionarios

    return publicaciones


def test_masivo_carga_json():
    publicaciones = leer_fichero_json_crossref()

    # Imprimir algunas publicaciones
    lista_datos_publicacion = []
    for publicacion in publicaciones:  # Mostrar las primeras 5
        parser = CrossrefParser(data=publicacion)
        datos = parser.datos_carga_publicacion
        lista_datos_publicacion.append(datos)

    estadisticas = estadisticas_datos_publicacion(lista_datos_publicacion)
    pprint.pp(estadisticas)
