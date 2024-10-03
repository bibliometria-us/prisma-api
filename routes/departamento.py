from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import comprobar_api_key
from utils.timing import func_timer as timer
import utils.response as response
import config.global_config as gconfig

departamento_namespace = Namespace("departamento", description="Departamentos")

global_responses = gconfig.responses

global_params = gconfig.params

columns = ["d.idDepartamento as id", "d.nombre"]


def get_departamento_from_id(id):
    query = (
        f"SELECT {', '.join(columns)} FROM i_departamento d WHERE d.idDepartamento = %s"
    )
    params = []
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@departamento_namespace.route("/")
class Departamento(Resource):
    @departamento_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "id": {
                "name": "ID",
                "description": "ID del departamento",
                "type": "int",
            },
        },
    )
    def get(self):
        """Información de un departamento"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("id", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=departamento_namespace)

        try:
            data = get_departamento_from_id(id)
        except:
            departamento_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=departamento_namespace,
            dict_selectable_column="id",
            object_name="departamento",
            xml_root_name=None,
        )


def get_departamentos(conditions, params):
    query = f"SELECT {', '.join(columns)} FROM i_departamento d"
    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@departamento_namespace.route("s/")
class Departamentos(Resource):
    @departamento_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "nombre": {
                "name": "Nombre",
                "description": "Nombre del departamento",
                "type": "str",
            },
        },
    )
    def get(self):
        """Búsqueda de departamentos"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        nombre = args.get("nombre", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=departamento_namespace)

        conditions = []
        params = []

        if nombre:
            conditions.append(
                "d.nombre COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(nombre)

        try:
            data = get_departamentos(conditions, params)
        except:
            departamento_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=departamento_namespace,
            dict_selectable_column="id",
            object_name="departamento",
            xml_root_name="departamentos",
        )
