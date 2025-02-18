from integration.apis.elsevier.scopus_search.scopus_search import ScopusSearch
from routes.carga.publicacion.scopus.parser import ScopusParser
import xml.etree.ElementTree as ET


ids_scopus = [
    "2-s2.0-85191998367",  # Artículo
    "2-s2.0-85201735741",  # Capítulo
    "2-s2.0-85201792216",  # Ponencia
    # "2-s2.0-85ccxcx2216 ",  # No existe
]


def test_busqueda_por_id():
    api = ScopusSearch()
    for id in ids_scopus:
        records = api.get_from_id(id)
        assert not api.response.get("service-error")
        for record in records:
            parser = ScopusParser(data=record)
            json = parser.datos_carga_publicacion.to_json()
