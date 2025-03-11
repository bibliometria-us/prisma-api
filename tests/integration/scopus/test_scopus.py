import pprint
import pytest
from integration.apis.elsevier.scopus_search.scopus_search import ScopusSearch
from routes.carga import consultas_cargas
from routes.carga.publicacion.scopus.parser import ScopusParser
import xml.etree.ElementTree as ET
import json
import os
import time
from collections import defaultdict

from tests.integration.utils.utils import estadisticas_datos_publicacion


ids_scopus = [
    "2-s2.0-85191998367",  # Artículo
    "2-s2.0-85201735741",  # Capítulo
    "2-s2.0-85201792216",  # Ponencia
    # "2-s2.0-85ccxcx2216 ",  # No existe
]

dois_scopus = [
    "10.12688/openreseurope.17554.2",  # Artículo
    "10.3233/faia240928",  # Capítulo
    "10.1007/978-3-030-61705-9_13",  # Ponencia
    # "2-s2.0-85ccxcx2216 ",  # No existe
]

id_inves_scopus = ["6506630834"]


def test_busqueda_por_id():
    api = ScopusSearch()
    for id in ids_scopus:
        records = api.get_publicaciones_por_id(id)
        assert not api.response.get("service-error")
        for record in records:
            parser = ScopusParser(data=record)
            json = parser.datos_carga_publicacion.to_json()


def test_busqueda_por_doi():
    api = ScopusSearch()
    for id in dois_scopus:
        records = api.get_publicaciones_por_doi(id)
        assert not api.response.get("service-error")
        for record in records:
            parser = ScopusParser(data=record)
            json = parser.datos_carga_publicacion.to_json()


# @pytest.mark.skip()
def test_masivo_por_inves():
    api = ScopusSearch()
    for id in id_inves_scopus:
        records = api.get_publicaciones_por_investigador_fecha(
            id, agno_inicio=None, agno_fin=None
        )
        assert not api.response.get("service-error")
        for record in records:
            parser = ScopusParser(data=record)
            json = parser.datos_carga_publicacion.to_json()


@pytest.mark.skipif(
    os.path.exists("tests/integration/scopus/json_masivo_scopus.json"),
    reason="JSON file already exists",
)
def test_masivo_guardado_json():
    """Obtiene publicaciones de investigadores activos y guarda en JSON en cada iteración."""
    fuente = "scopus"
    api = ScopusSearch()
    lista_id_inves = consultas_cargas.get_investigadores_activos()

    FILENAME = f"tests/integration/{fuente}/json_masivo_{fuente}.json"
    PROGRESS_FILE = f"tests/integration/{fuente}/progreso_{fuente}.json"

    # Cargar JSON de publicaciones (si existe)
    if os.path.exists(FILENAME):
        with open(FILENAME, "r", encoding="utf-8") as file:
            try:
                publicaciones = json.load(file)
                if not isinstance(publicaciones, list):
                    publicaciones = []
            except json.JSONDecodeError:
                publicaciones = []
    else:
        publicaciones = []

    # Cargar progreso de investigadores procesados
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as file:
            try:
                progreso = set(json.load(file))
            except json.JSONDecodeError:
                progreso = set()
    else:
        progreso = set()

    # Procesar cada investigador
    for key, value in list(lista_id_inves.items())[:1000]:
        if key in progreso:
            print(f"Investigador {key} ya procesado. Saltando...")
            continue

        print(f"Obteniendo publicaciones de {key}...")
        ids = consultas_cargas.get_id_investigadores(key)
        if any(d["tipo"] == fuente for d in ids.values()):
            intentos = 3  # Intentos máximos en caso de fallo
            while intentos > 0:
                try:

                    id_fuente = next(
                        (v for v in ids.values() if v.get("tipo") == fuente), None
                    )
                    records = api.get_publicaciones_por_investigador_fecha(
                        id_fuente.get("valor"), agno_inicio=None, agno_fin=None
                    )
                    publicaciones.extend(records)

                    # Guardar datos y progreso después de cada investigador
                    with open(FILENAME, "w", encoding="utf-8") as file:
                        json.dump(publicaciones, file, indent=4, ensure_ascii=False)

                    progreso.add(key)
                    with open(PROGRESS_FILE, "w", encoding="utf-8") as file:
                        json.dump(list(progreso), file, indent=4, ensure_ascii=False)

                    print(f"Publicaciones de {key} guardadas.")
                    break  # Salir del bucle si la llamada fue exitosa

                except Exception as e:
                    print(f"Error con {key}: {e}. Reintentando...")
                    intentos -= 1
                    time.sleep(2)  # Espera antes de reintentar

            if intentos == 0:
                print(
                    f"No se pudo obtener publicaciones para {key} tras varios intentos."
                )

    # Eliminar duplicados al final
    print("Eliminando duplicados finales...")
    publicaciones_unicas = list(
        {json.dumps(pub, sort_keys=True) for pub in publicaciones}
    )
    publicaciones_unicas = [json.loads(pub) for pub in publicaciones_unicas]

    with open(FILENAME, "w", encoding="utf-8") as file:
        json.dump(publicaciones_unicas, file, indent=4, ensure_ascii=False)

    # Borrar progreso porque ya terminó
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

    print(
        f"Se han guardado {len(publicaciones_unicas)} publicaciones únicas en {FILENAME}."
    )


def test_masivo_carga_json():
    fuente = "scopus"
    FILENAME = f"tests/integration/{fuente}/json_masivo_{fuente}.json"
    publicaciones_parseadas = []

    # Cargar el archivo JSON
    with open(FILENAME, "r", encoding="utf-8") as file:
        publicaciones = json.load(file)  # Lista de diccionarios

    # Verificar la estructura
    print(type(publicaciones))  # Debería ser <class 'list'>
    print(type(publicaciones[0]))  # Cada elemento debería ser <class 'dict'>

    lista_datos_publicacion = []
    # Imprimir algunas publicaciones
    for publicacion in publicaciones:  # Mostrar las primeras 5
        parser = ScopusParser(data=publicacion)
        datos = parser.datos_carga_publicacion
        lista_datos_publicacion.append(datos)

    estadisticas = estadisticas_datos_publicacion(lista_datos_publicacion)
    pprint.pp(estadisticas)
