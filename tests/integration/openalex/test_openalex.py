from time import sleep
from integration.apis.openalex.openalex import OpenalexAPI
from routes.carga.publicacion.openalex.parser import OpenalexParser
import xml.etree.ElementTree as ET


ids_openalex = [
    "10.12688/openreseurope.17554.2",  # Artículo
    "10.3233/faia240928",  # Capítulo
    "10.1007/978-3-030-61705-9_13",  # Ponencia
    # "2-s2.0-85ccxcx2216 ",  # No existe
]


def test_busqueda_por_doi():
    api = OpenalexAPI()
    for id in ids_openalex:
        sleep(1)
        records = api.search_get_from_doi(id)
        assert not api.response.get("service-error")
        for record in records:
            parser = OpenalexParser(data=record)
            json = parser.datos_carga_publicacion.to_json()


def test_busqueda_por_id():
    api = OpenalexAPI()
    for id in ids_openalex:
        sleep(1)
        records = api.search_get_from_id(id)
        assert not api.response.get("service-error")
        for record in records:
            parser = OpenalexParser(data=record)
            json = parser.datos_carga_publicacion.to_json()
