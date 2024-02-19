from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import comprobar_api_key
from utils.timing import func_timer as timer
import utils.pages as pages
import utils.response as response
import utils.date as date
import config.global_config as gconfig

editorial_namespace = Namespace("editorial", description="Editoriales")


global_responses = gconfig.responses

global_params = gconfig.params

paginate_params = gconfig.paginate_params


def get_editorial_from_id(id):
    columns = ["e.id", "e.nombre", "e.tipo", "e.pais", "e.url", "e.visible"]

    query = f"SELECT {', '.join(columns)} FROM p_editor e WHERE e.id = %s"
    params = []
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@editorial_namespace.route("/")
class Editorial(Resource):
    @editorial_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "editorial": {
                "name": "Editorial",
                "description": "ID de la editorial",
                "type": "int",
            },
        },
    )
    def get(self):
        """Información de un editorial"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("editorial", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=editorial_namespace)

        try:
            data = get_editorial_from_id(id)
        except:
            editorial_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=editorial_namespace,
            dict_selectable_column="id",
            object_name="editorial",
            xml_root_name=None,
        )


@editorial_namespace.route("/fuentes/")
class FuentesEditorial(Resource):
    @editorial_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "editorial": {
                "name": "Editorial",
                "description": "ID de la editorial",
                "type": "int",
            },
        },
    )
    def get(self):
        """Fuentes de una editorial"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("editorial", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=editorial_namespace)

        request_url = f"http://{request.host}/"
        request_urn = (
            f"fuentes/?salida={accept_type}&api_key={api_key}&id_editorial={id}"
        )
        referrer = request.referrer
        return response.generate_response_from_uri(request_url, request_urn, referrer)


@editorial_namespace.route("/publicaciones/")
class PublicacionesEditorial(Resource):
    @editorial_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "editorial": {
                "name": "Editorial",
                "description": "ID de la editorial",
                "type": "int",
            },
        },
    )
    def get(self):
        """Publicaciones de una editorial"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("editorial", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=editorial_namespace)

        request_url = f"http://{request.host}/"
        request_urn = f"publicaciones/?salida={accept_type}&api_key={api_key}&longitud_pagina=0&editorial={id}"
        referrer = request.referrer
        return response.generate_response_from_uri(request_url, request_urn, referrer)


def get_metricas_from_editorial(id, tipo):

    def merge_queries(select, table_name):
        select_string = ", ".join(f"{value} as {key}" for key, value in select.items())
        query = f"SELECT {select_string} FROM {table_name} m"
        query += (
            f" WHERE m.editorial = (SELECT e.nombre from p_editor e WHERE e.id = %s)"
        )

        return query

    queries = {
        "spi": merge_queries(
            select={
                "id": "m.idMetrica",
                "editorial": "m.editorial",
                "editorial_original": "m.editorial_original",
                "año": "m.agno",
                "categoria": "m.categoria",
                "puntuacion": "CAST(m.puntuacion AS DOUBLE)",
                "posicion": "m.posicion",
                "total_ed": "m.total_ed",
                "cuartil": "m.cuartil",
            },
            table_name="m_spi",
        ),
        "csic": merge_queries(
            select={
                "id": "m.id",
                "editorial": "m.editorial",
                "puntuacion": "m.puntuacion",
            },
            table_name="m_csic",
        ),
    }
    query = queries[tipo]
    params = [id]
    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)
    return result


@editorial_namespace.route("/metricas/")
class MetricasEditorial(Resource):
    @editorial_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "editorial": {
                "name": "Fuente",
                "description": "ID de la editorial",
                "type": "int",
            },
            "tipo": {
                "name": "Tipo",
                "description": "Tipo de métrica",
                "type": "str",
                "enum": ["SPI", "CSIC"],
                "default": "SPI",
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
        id = args.get("editorial", None)
        tipo = args.get("tipo", "spi").lower()

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=editorial_namespace)

        try:
            data = get_metricas_from_editorial(id, tipo)
        except:
            editorial_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=editorial_namespace,
            dict_selectable_column="id",
            object_name="metrica",
            xml_root_name="metricas",
        )
