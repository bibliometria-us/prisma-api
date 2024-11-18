from integration.apis.elsevier.scopus_search.scopus_search import ScopusSearch
from routes.carga.publicacion.scopus.parser import ScopusParser
import xml.etree.ElementTree as ET


handles = [
    "85048756127",
]

""" "85032710576",
    "85040710952",
    "85018267667",
    "85011039405", """


def test_busqueda_por_id():
    api = ScopusSearch()
    for handle in handles:
        api.get_from_id(handle)
        assert not api.response.get("service-error")


def test_error_busqueda():
    api = ScopusSearch()
    for handle in handles:
        api.args["qwuery"] = "query"
        api.search()
        assert api.response.get("service-error")


def test_get_json():
    api = ScopusSearch()
    for handle in handles:
        api.get_from_id(handle)
        assert not api.response.get("service-error")


""" def test_busqueda_por_handle():
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
 """
