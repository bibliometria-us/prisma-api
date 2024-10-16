from integration.apis.idus.idus import IdusAPIItems, IdusAPISearch
from routes.carga.publicacion.idus.parser import IdusParser
import xml.etree.ElementTree as ET

from routes.carga.publicacion.idus.xml_doi import xmlDoiIdus

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
        response = api.get_from_handle(handle)
        assert not response.get("error")


def test_item_por_handle():
    api = IdusAPIItems()
    for handle in handles:
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
