import logging
from db.conexion import BaseDatos
from routes.informes.pub_metrica.pub_metrica import generar_informe
from routes.informes.utils import normalize_id_list
import os
import shutil
import pytest
# Obtiene toda la lista de una determinada fuente (departamento, grupo, instituto...)


def get_fuentes(tipo):
    tipo_to_columna = {
        "departamento": "idDepartamento",
        "grupo": "idGrupo",
        "instituto": "idInstituto",
    }
    tipo_to_tabla = {
        "departamento": "i_departamento",
        "grupo": "i_grupo",
        "instituto": "i_instituto",
    }
    query = "SELECT {columna} FROM {tabla} GROUP BY {columna}"
    query = query.format(
        **{"columna": tipo_to_columna[tipo], "tabla": tipo_to_tabla[tipo]})

    db = BaseDatos()
    datos = db.ejecutarConsulta(query)

    result = [dato[0] for dato in datos[1:]]
    return result


def test_pub_metrica():
    fuentes = {
        "departamento": get_fuentes("departamento"),
        "grupo": get_fuentes("grupo"),
        "instituto": get_fuentes("instituto"), }

    for nombre_fuente, fuentes in fuentes.items():
        for fuente in fuentes:
            for tipo in ("pdf", "excel"):
                logging.info(f"Prueba de informe de {nombre_fuente}: {fuente}")
                filename = f"tests/temp/prueba_{nombre_fuente}_{fuente}"
                tipo_a_formato = {"pdf": "pdf", "excel": "xlsx"}
                filename_formato = f"{filename}.{tipo_a_formato[tipo]}"
                if os.path.exists(filename_formato):
                    logging.info(
                        f"Informe de {nombre_fuente}: {fuente} ya creado")
                else:
                    try:
                        generar_informe(fuentes={nombre_fuente: fuente},
                                        año_inicio=2022, año_fin=2023, tipo=tipo, filename=filename)
                    except Exception as e:
                        pytest.fail(
                            f"Error en el informe de {nombre_fuente}: {fuente}. \n {e}")

    logging.info("Test completado")
    shutil.rmtree("tests/temp/")
    os.makedirs("tests/temp/", exist_ok=True)
