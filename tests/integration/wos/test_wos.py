from time import sleep
from integration.apis.clarivate.wos.wos_api import WosAPI
from routes.carga.publicacion.wos.parser import WosParser
import xml.etree.ElementTree as ET


ids_wos = [
    # "001218226500001",  # Artículo
    # "000269306400009",  # Capítulo
    "000440729700013",  # Ponencia
    # "2-s2.0-85ccxcx2216 ",  # No existe
]


def test_busqueda_por_id():
    api = WosAPI()
    for id in ids_wos:
        sleep(1)
        records = api.get_from_id(id)
        assert not api.response.get("service-error")
        for record in records:
            parser = WosParser(data=record)
            json = parser.datos_carga_publicacion.to_json()
