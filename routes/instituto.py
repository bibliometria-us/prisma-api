from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import comprobar_api_key
from utils.timing import func_timer as timer
import utils.response as response
import config.global_config as gconfig

instituto_namespace = Namespace("instituto", description="Institutos de investigación")

global_responses = gconfig.responses

global_params = gconfig.params

columns = ["i.idInstituto as id", "i.nombre", "i.acronimo"]


def get_instituto_from_id(id):
    query = f"SELECT {', '.join(columns)} FROM i_instituto i WHERE i.idInstituto = %s"
    params = []
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@instituto_namespace.route("/")
class Instituto(Resource):
    @instituto_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "id": {
                "name": "ID",
                "description": "ID del instituto",
                "type": "int",
            },
        },
    )
    def get(self):
        """Información de un instituto"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("id", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=instituto_namespace)

        try:
            data = get_instituto_from_id(id)
        except:
            instituto_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=instituto_namespace,
            dict_selectable_column="id",
            object_name="instituto",
            xml_root_name=None,
        )


def get_institutos(conditions, params):
    query = f"SELECT {', '.join(columns)} FROM i_instituto i"
    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@instituto_namespace.route("s/")
class Institutos(Resource):
    @instituto_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "nombre": {
                "name": "Nombre",
                "description": "Nombre del instituto",
                "type": "str",
            },
            "acronimo": {
                "name": "Acrónimo",
                "description": "Acrónimo del instituto",
                "type": "str",
            },
        },
    )
    def get(self):
        """Búsqueda de institutos"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        nombre = args.get("nombre", None)
        acronimo = args.get("acronimo", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=instituto_namespace)

        conditions = []
        params = []

        if nombre:
            conditions.append(
                "i.nombre COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(nombre)

        if acronimo:
            conditions.append("i.acronimo = %s")
            params.append(acronimo.upper())

        try:
            data = get_institutos(conditions, params)
        except:
            instituto_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=instituto_namespace,
            dict_selectable_column="id",
            object_name="instituto",
            xml_root_name="institutos",
        )
