from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import comprobar_api_key
from utils.timing import func_timer as timer
import utils.pages as pages
import utils.response as response
import config.global_config as gconfig

fuente_namespace = Namespace("fuente", description="Fuentes de publicaciones")

global_responses = gconfig.responses

global_params = gconfig.params

paginate_params = gconfig.paginate_params

columns = ["f.idFuente as id", "f.tipo", "f.titulo", "f.editorial"]

count_columns = ["COUNT(*) as cantidad"]

# OBTENER FUENTE POR ID


def get_fuente_from_id(id):
    query = f"SELECT {', '.join(columns)} FROM p_fuente f" + " WHERE f.idFuente = %s"
    params = []
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@fuente_namespace.route("/")
class Fuente(Resource):
    @fuente_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "fuente": {
                "name": "Fuente",
                "description": "ID de la fuente",
                "type": "int",
            },
        },
    )
    def get(self):
        """Información de una fuente"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("fuente", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=fuente_namespace)

        try:
            data = get_fuente_from_id(id)
        except:
            fuente_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=fuente_namespace,
            dict_selectable_column="id",
            object_name="fuente",
            xml_root_name=None,
        )


# BUSCADOR DE FUENTES


def get_fuentes(columns, conditions, params, limit=None, offset=None):
    query = (
        f"SELECT {', '.join(columns)} FROM p_fuente f WHERE {' AND '.join(conditions)}"
    )

    if limit is not None and offset is not None:
        query += " LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@fuente_namespace.route("s/")
class Fuentes(Resource):
    @fuente_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            **paginate_params,
            "inactivos": {
                "name": "ID",
                "description": "Incluir fuentes inactivas",
                "type": "bool",
                "enum": ["True", "False"],
            },
            "titulo": {
                "name": "Título",
                "description": "Título de la fuente",
                "type": "int",
            },
            "identificador": {
                "name": "Identificador",
                "description": "Identificador de la fuente (ISSN, eISSN, ISBN, eISBN, DOI...)",
                "type": "str",
            },
            "nombre_editorial": {
                "name": "Editorial",
                "description": "Nombre de la editorial a la que pertenece la fuente",
                "type": "str",
            },
            "id_editorial": {
                "name": "Editorial ID",
                "description": "ID de la editorial a la que pertenece la fuente",
                "type": "int",
            },
        },
    )
    def get(self):
        """Búsqueda de fuentes"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        pagina = int(args.get("pagina", 1))
        inactivos = (
            True if (args.get("inactivos", "False").lower() == "true") else False
        )
        longitud_pagina = int(args.get("longitud_pagina", 100))
        titulo = args.get("titulo", None)
        identificador = args.get("identificador", None)
        nombre_editorial = args.get("nombre_editorial", None)
        id_editorial = args.get("id_editorial", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=fuente_namespace)

        conditions = []
        params = []
        if not inactivos:
            conditions.append("f.eliminado = 0")
        if titulo:
            conditions.append(
                "f.titulo COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(titulo)
        if identificador:
            conditions.append(
                "f.idFuente IN (SELECT idf.idFuente FROM p_identificador_fuente idf WHERE idf.valor = %s)"
            )
            params.append(identificador)
        if nombre_editorial:
            conditions.append(
                "f.editorial COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(nombre_editorial)
        if id_editorial:
            conditions.append(
                "f.editorial = (SELECT nombre FROM p_editor WHERE id = %s)"
            )
            params.append(id_editorial)

        amount = int(get_fuentes(count_columns, conditions, params)[1][0])

        limit, offset = pages.get_page_offset(pagina, longitud_pagina, amount)

        try:
            data = get_fuentes(columns, conditions, params, limit, offset)
        except:
            fuente_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=fuente_namespace,
            dict_selectable_column="id",
            object_name="fuente",
            xml_root_name="fuentes",
        )


# OBTENER IDENTIFICADORES DE UNA FUENTE


def get_identificadores_from_fuente(id):
    columns = ["idf.idIdentificador as id, idf.tipo, idf.valor"]

    query = (
        f"SELECT {', '.join(columns)} FROM p_identificador_fuente idf "
        + " WHERE idf.idFuente = %s"
    )
    params = []
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@fuente_namespace.route("/identificadores/")
class IdentificadoresFuente(Resource):
    @fuente_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "fuente": {
                "name": "Fuente",
                "description": "ID de la fuente",
                "type": "int",
            },
        },
    )
    def get(self):
        """Identificadores de una fuente (ISSN, eISSN, ISBN, eISBN, DOI)"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("fuente", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=fuente_namespace)

        try:
            data = get_identificadores_from_fuente(id)
        except:
            fuente_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=fuente_namespace,
            dict_selectable_column="id",
            object_name="identificador",
            xml_root_name="identificadores",
        )


# OBTENER PUBLICACIONES DE UNA FUENTE


@fuente_namespace.route("/publicaciones/")
class PublicacionesFuente(Resource):
    @fuente_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "estadisticas": {
                "name": "Estadísticas",
                "description": "Si se activa, se devolverá un resumen de estadísticas de la búsqueda",
                "type": "bool",
                "enum": ["True", "False"],
            },
            "fuente": {
                "name": "Fuente",
                "description": "ID de la fuente",
                "type": "int",
            },
            "coleccion": {
                "name": "Colección",
                "description": "ID de la colección por la que filtrar",
                "type": "int",
            },
        },
    )
    def get(self):
        """Publicaciones de una fuente"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)

        estadisticas = (
            True if (args.get("estadisticas", "False").lower() == "true") else False
        )
        id = args.get("fuente", None)
        coleccion = args.get("coleccion", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=fuente_namespace)

        request_url = f"http://{request.host}/"
        request_urn = (
            f"publicaciones/?salida={accept_type}&api_key={api_key}&fuente={response.empty_string_if_none(id)}"
            + f"&longitud_pagina=0&estadisticas={estadisticas}&coleccion={response.empty_string_if_none(coleccion)}"
        )
        referrer = request.referrer
        return response.generate_response_from_uri(request_url, request_urn, referrer)


# MÉTRICAS DE PUBLICACIONES


def get_metricas_publicacion(id_fuente: int, tipo: str, año: int):
    params = []

    def merge_queries(table_name, select, issn_amount, issn_2_name="issn_2"):
        check_issn_2 = (
            f"OR {issn_2_name} IN (SELECT valor FROM lista_issn)"
            if issn_amount == 2
            else ""
        )
        select_string = ", ".join(f"{value} as {key}" for key, value in select.items())
        group = f"GROUP BY {select['revista']}, {select['categoria']}, {select['id']}"

        result = f"""
        {lista_issns}
        SELECT {select_string} FROM {table_name} AS m
        WHERE
        (issn IN (SELECT valor FROM lista_issn) {check_issn_2})
        {f" AND {select['año']} = %s" if año else ""}
        {group}
        {order}
        """

        return result

    # Consulta que almacena todos los posibles ISSNs asociados a la publicación
    lista_issns = """
    WITH lista_issn AS (
    SELECT i.valor
    FROM p_identificador_fuente i
    WHERE 
        i.tipo IN ('eissn', 'issn')
        AND i.idFuente = %s
    )
    """
    order = "ORDER BY año ASC, categoria ASC"

    queries = {
        "jif": merge_queries(
            select={
                "id": "m.id_jcr",
                "revista": "m.journal",
                "año": "m.year",
                "edicion": "m.edition",
                "categoria": "m.category",
                "valor": "CAST(m.impact_factor AS DOUBLE)",
                "posicion": "m.rank",
                "cuartil": "m.quartile",
                "decil": "m.decil",
                "tercil": "m.tercil",
            },
            table_name="m_jcr",
            issn_amount=2,
        ),
        "jci": merge_queries(
            select={
                "id": "m.id",
                "revista": "m.revista",
                "año": "m.agno",
                "categoria": "m.categoria",
                "valor": "CAST(m.jci AS DOUBLE)",
                "posicion": "m.posicion",
                "cuartil": "m.cuartil",
                "decil": "m.decil",
                "tercil": "m.tercil",
                "percentil": "m.percentil",
            },
            table_name="m_jci",
            issn_amount=1,
        ),
        "citescore": merge_queries(
            select={
                "id": "m.id",
                "revista": "m.revista",
                "año": "m.agno",
                "categoria": "m.categoria",
                "valor": "CAST(m.citeScore AS DOUBLE)",
                "posicion": "m.posicion",
                "cuartil": "m.cuartil",
                "decil": "m.decil",
                "tercil": "m.tercil",
            },
            table_name="m_citescore",
            issn_amount=1,
        ),
        "sjr": merge_queries(
            select={
                "id": "m.id_sjr",
                "revista": "m.journal",
                "año": "m.year",
                "categoria": "m.category",
                "valor": "CAST(m.impact_factor AS DOUBLE)",
                "posicion": "m.rank",
                "cuartil": "m.quartile",
                "decil": "m.decil",
                "tercil": "m.tercil",
            },
            table_name="m_sjr",
            issn_amount=2,
        ),
        "idr": merge_queries(
            select={
                "id": "m.id",
                "revista": "m.titulo",
                "año": "m.anualidad",
                "categoria": "m.categoria",
                "valor": "CAST(m.factorImpacto AS DOUBLE)",
                "posicion": "m.posicion",
                "cuartil": "m.cuartil",
                "percentil": "m.percentil",
            },
            table_name="m_idr",
            issn_amount=1,
        ),
        "fecyt": merge_queries(
            select={
                "id": "m.id",
                "revista": "m.titulo",
                "año": "m.agno",
                "categoria": "m.categoria",
                "valor": "CAST(m.puntuacion AS DOUBLE)",
                "posicion": "m.posicion",
                "cuartil": "m.cuartil",
                "convocatoria": "m.convocatoria",
                "url": "m.url",
            },
            table_name="m_fecyt",
            issn_amount=2,
            issn_2_name="eissn",
        ),
    }
    query = queries[tipo]

    params.append(id_fuente)
    if año:
        params.append(año)
    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@fuente_namespace.route("/metricas/")
class MetricasFuente(Resource):
    @fuente_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "fuente": {
                "name": "Fuente",
                "description": "ID de la fuente",
                "type": "int",
            },
            "tipo": {
                "name": "Tipo",
                "description": "Indicio de calidad a solicitar",
                "type": "int",
                "enum": ["JIF", "JCI", "CiteScore", "SJR", "IDR", "FECYT"],
                "default": "JIF",
            },
            "año": {
                "name": "Año",
                "description": "Año de la métrica",
                "type": "int",
            },
        },
    )
    def get(self):
        headers = request.headers
        args = request.args

        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        fuente = args.get("fuente", None)
        tipo = args.get("tipo", "").lower()
        año = args.get("año", None)

        comprobar_api_key(api_key=api_key, namespace=fuente_namespace)

        try:
            data = get_metricas_publicacion(fuente, tipo, año)
        except:
            fuente_namespace.abort(500, "Error del servidor")

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=fuente_namespace,
            dict_selectable_column="id",
            object_name="metrica",
            xml_root_name="metricas",
        )
