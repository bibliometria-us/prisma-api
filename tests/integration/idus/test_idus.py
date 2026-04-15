import os
import pprint
from time import sleep

from flask import json
import pytest
from db.conexion import BaseDatos
from integration.apis.idus.idus import IdusAPI, IdusAPIItems, IdusAPISearch
from routes.carga.publicacion.datos_carga_publicacion import DatosCargaPublicacion
from routes.carga.publicacion.idus.parser import IdusParser
import xml.etree.ElementTree as ET

from routes.carga.publicacion.idus.xml_doi import xmlDoiIdus
from tests.integration.utils.utils import estadisticas_datos_publicacion

handles = [
    "11441/82033",
    "11441/41044",
    "11441/47752",
    "11441/47848",
    "11441/58204",
    "11441/72890",
    "11441/22655",
    "11441/97814",
    "11441/40766",
    "11441/74699",
    "11441/71373",
    "11441/18198",
    "11441/95195",
    "11441/109286",
]


def test_busqueda_por_handle():
    api = IdusAPISearch()
    for handle in handles:
        sleep(0.5)
        response = api.get_from_handle(handle)
        assert not response.get("error")


def test_item_por_handle():
    api = IdusAPIItems()
    for handle in handles:
        sleep(0.5)
        response = api.get_from_handle(handle)
        assert not response.get("error")


def test_parser():
    for handle in handles:
        parser = IdusParser(handle=handle)
        json = parser.datos_carga_publicacion.to_json()
        pass


def test_doi_xml():
    for handle in handles:
        xml_doi = xmlDoiIdus(handle)
        xml = xml_doi.xml
        ET.fromstring(xml)


@pytest.mark.skipif(
    os.path.exists("tests/integration/test_idus/json_masivo_idus.json"),
    reason="JSON file already exists",
)
def test_masivo_guardado_json():
    api = IdusAPIItems()

    query = """SELECT valor FROM p_identificador_publicacion WHERE tipo = 'idus' ORDER BY RAND() LIMIT %(limit)s"""
    params = {"limit": 1000}

    bd = BaseDatos()
    bd.ejecutarConsulta(query, params)
    df = bd.get_dataframe()

    handles = df["valor"].tolist()
    records = []

    for handle in handles:
        record = api.get_from_handle(handle)
        if record:
            records.append(record)

    fuente = "idus"
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


def leer_fichero_json_idus() -> list[DatosCargaPublicacion]:
    fuente = "idus"
    FILENAME = f"tests/integration/{fuente}/json_masivo_{fuente}.json"

    # Cargar el archivo JSON
    with open(FILENAME, "r", encoding="utf-8") as file:
        publicaciones = json.load(file)  # Lista de diccionarios

    return publicaciones


def test_masivo_carga_json():
    publicaciones = leer_fichero_json_idus()

    # Imprimir algunas publicaciones
    lista_datos_publicacion = []
    for publicacion in publicaciones:  # Mostrar las primeras 5
        parser = IdusParser(data=publicacion)
        datos = parser.datos_carga_publicacion
        lista_datos_publicacion.append(datos)

    estadisticas = estadisticas_datos_publicacion(lista_datos_publicacion)
    pprint.pp(estadisticas)
