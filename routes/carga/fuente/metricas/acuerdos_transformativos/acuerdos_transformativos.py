from datetime import datetime
import numpy as np
from pandas import DataFrame
import pandas as pd
from werkzeug.datastructures import FileStorage
from db.conexion import BaseDatos
from routes.carga.fuente.metricas.acuerdos_transformativos.exception import (
    ErrorColumnasFicheroAT,
    ErrorTransformacionFicheroAT,
    ErrorValoresFicheroAT,
)
import routes.carga.fuente.metricas.clarivate_journals as clarivate_journals

columnas_obligatorias = {
    "Editorial": "Editorial",
    "ISSN": "ISSN",
    "eISSN": "eISSN",
    "Título": "Título",
    "Modelo de publicación": "Tipo",
}

tipos_at = {
    "Sólo Open Access": "Sólo Open Access",
    "Sólo Open Access (depende de la disponibilidad de licencias)": "Sólo Open Access",
    "Híbrida": "Híbrida",
    "Cambio de híbrida a OA": "Cambio de híbrida a OA",
    "Cambiando a acceso abierto (depende de la disponibilidad de APCs)": "Cambio de híbrida a OA",
    "Diamante (publicación sin tasas)": "Diamante",
    "Research Open (cubierta por el acuerdo)": "Research Open",
    "Subscribe to Open (S2O)": "Subscribe to Open",
}


def transformar_fichero(file: FileStorage) -> DataFrame:
    try:
        data = pd.read_excel(file)
    except Exception:
        raise ErrorTransformacionFicheroAT(f"Error inesperado al procesar el fichero")

    verificar_columnas(data=data)
    data = normalizar_columnas(data=data)

    verificar_tipos(data=data)
    data = normalizar_tipos(data=data)

    data = normalizar_issns(data=data)

    return data


def verificar_columnas(data: DataFrame) -> None:
    columnas_fichero = set(data.columns)
    missing = set(columnas_obligatorias.keys()) - columnas_fichero

    if missing:
        raise ErrorColumnasFicheroAT(
            f"El fichero no contiene las columnas {str(missing)}"
        )


def normalizar_columnas(data: DataFrame) -> DataFrame:
    data = data.rename(columns=columnas_obligatorias)
    return data


def verificar_tipos(data: DataFrame) -> None:
    valores_validos = set(tipos_at.keys())
    columna = "Tipo"

    is_valid = data[columna].isin(valores_validos).all()

    if not is_valid:
        raise ErrorValoresFicheroAT(
            f"Se han encontrado valores en la columna '{columna}' no válidos. Los valores válidos son {valores_validos}"
        )


def normalizar_tipos(data: DataFrame) -> DataFrame:
    data["Tipo"] = data["Tipo"].map(tipos_at)
    return data


def normalizar_issns(data: DataFrame) -> DataFrame:
    issn_pattern = r"^\d{4}-\d{3}[\dX]$"
    cols_to_check = ["ISSN", "eISSN"]

    for col in cols_to_check:
        data[col] = data[col].fillna("")
        data[col] = data[col].str.strip()

        data[col] = data[col].mask(
            ~data[col].str.contains(issn_pattern, na=False), None
        )

    return data


def carga_acuerdos_transformativos(
    data: DataFrame, inicio_wos: int = None, final_wos: int = None
):

    for revista in data.itertuples():
        if not (revista.ISSN or revista.eISSN):
            continue

        id_fuente = buscar_revista(revista=revista)

        if not id_fuente:
            id_fuente = insertar_revista(revista=revista)

        if not id_fuente:
            continue

        insertar_at(id_fuente=id_fuente, revista=revista)

    actualizar_citescore()
    actualizar_metricas_wos(inicio_wos=inicio_wos, final_wos=final_wos)


def buscar_revista(revista) -> int:
    bd = BaseDatos()

    issns = list(filter(None, [revista.ISSN, revista.eISSN]))
    str_issns = f"({','.join(f"'{issn}'" for issn in issns)})"

    query_busqueda_revista = f"""
    SELECT pf.idFuente from p_fuente pf
    LEFT JOIN (SELECT idFuente, valor FROM p_identificador_fuente WHERE tipo IN ('issn','eissn')) issn ON issn.idFuente = pf.idFuente
    WHERE issn.valor IN {str_issns}
    GROUP BY pf.idFuente
    """

    bd.ejecutarConsulta(query_busqueda_revista)
    id_fuente = bd.get_first_cell()

    return id_fuente


def insertar_revista(revista) -> int:
    bd = BaseDatos(keep_connection_alive=True, autocommit=False)

    query = """
    INSERT INTO p_fuente (tipo, titulo, editorial, origen)
    VALUES (%(tipo)s, %(titulo)s, %(editorial)s, %(origen)s)
    """

    params = {
        "tipo": "Revista",
        "titulo": revista.Título,
        "editorial": revista.Editorial,
        "origen": "ACUERDOS TRANSFORMATIVOS",
    }

    bd.ejecutarConsulta(query, params=params)
    id_fuente = bd.last_id

    insertar_issns(revista=revista, id_fuente=id_fuente, bd=bd)

    bd.commit()
    bd.closeConnection()

    return id_fuente


def insertar_issns(revista, id_fuente: int, bd: BaseDatos):
    issns = {
        "issn": revista.ISSN,
        "eissn": revista.eISSN,
    }

    for tipo, valor in issns.items():
        if not valor:
            continue

        query = """
        INSERT INTO p_identificador_fuente (idFuente, tipo, valor)
        VALUES (%(idFuente)s, %(tipo)s, %(valor)s)
        """

        params = {"idFuente": id_fuente, "tipo": tipo, "valor": valor}

        bd.ejecutarConsulta(query, params=params)


def insertar_at(id_fuente: int, revista):
    bd = BaseDatos()

    query = """
    INSERT INTO m_at (idFuente, titulo, editorial, tipo, agno)
    VALUES (%(idFuente)s, %(titulo)s, %(editorial)s, %(tipo)s, %(agno)s)
    """

    params = {
        "idFuente": id_fuente,
        "titulo": revista.Título,
        "editorial": revista.Editorial,
        "tipo": revista.Tipo,
        "agno": datetime.now().year,
    }

    bd.ejecutarConsulta(query, params=params)


def actualizar_metricas_wos(inicio_wos: int, final_wos: int):
    bd = BaseDatos()

    current_year = datetime.now().year
    inicio_wos = inicio_wos or current_year - 2
    final_wos = final_wos or current_year - 2

    query = """SELECT m_at.idFuente FROM m_at WHERE agno = %(agno)s"""
    params = {"agno": current_year}

    bd.ejecutarConsulta(query, params=params)
    df = bd.get_dataframe()

    lista_ids_fuente = df["idFuente"].to_list()
    ids_fuente = ",".join(str(id_fuente) for id_fuente in lista_ids_fuente)

    clarivate_journals.iniciar_carga(
        fuentes=ids_fuente, año_inicio=inicio_wos, año_fin=final_wos
    )


def actualizar_citescore():
    bd = BaseDatos()

    # Para cada fila de la tabla m_citescore con idFuente nula, se busca idFuente en la tabla de identificadores que coincidan con el issn.
    query = """
    UPDATE m_citescore mc
    SET idFuente = (SELECT pif.idFuente FROM p_identificador_fuente pif WHERE pif.tipo IN ('issn', 'eissn') AND pif.valor = mc.issn LIMIT 1)
    WHERE mc.idFuente is NULL
    """

    bd.ejecutarConsulta(query)
