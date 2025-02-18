from integration.apis.crossref.crossref.crossref import CrossrefAPI
from routes.carga.publicacion.crossref.parser import CrossrefParser
import xml.etree.ElementTree as ET


ids_crossref = [
    "10.12688/openreseurope.17554.2",  # Artículo
    "10.3233/faia240928",  # Capítulo
    "10.1007/978-3-030-61705-9_13",  # Ponencia
    # "2-s2.0-85ccxcx2216 ",  # No existe
]


def test_busqueda_por_id():
    api = CrossrefAPI()
    for id in ids_crossref:
        record = api.get_from_doi(id)
        assert not api.response.get("service-error")
        parser = CrossrefParser(data=record)
        json = parser.datos_carga_publicacion.to_json()
