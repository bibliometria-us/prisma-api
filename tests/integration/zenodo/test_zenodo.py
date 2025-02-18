from integration.apis.zenodo.zenodo import ZenodoAPI
from routes.carga.publicacion.zenodo.parser import ZenodoParser
import xml.etree.ElementTree as ET


ids_zenodo = [
    "10.12688/openreseurope.17554.2",  # Artículo
    "10.3233/faia240928",  # Capítulo
    "10.1007/978-3-030-61705-9_13",  # Ponencia
    # "2-s2.0-85ccxcx2216 ",  # No existe
]


def test_busqueda_por_id():
    api = ZenodoAPI()
    for id in ids_zenodo:
        records = api.search_by_doi(id)
        assert not api.response.get("service-error")
        for record in records:
            parser = ZenodoParser(data=record)
            json = parser.datos_carga_publicacion.to_json()
