from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import comprobar_api_key
from utils.timing import func_timer as timer
import utils.response as response
import config.global_config as gconfig

grupo_namespace = Namespace("grupo", description="Grupos de investigación")

global_responses = gconfig.responses

global_params = gconfig.params

columns = ["g.idGrupo as id", "g.nombre", "g.acronimo", "g.rama", "g.codigo"]


def get_grupo_from_id(id):
    query = f"SELECT {', '.join(columns)} FROM i_grupo g WHERE g.idGrupo = %s"
    params = []
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@grupo_namespace.route("/")
class Grupo(Resource):
    @grupo_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "id": {
                "name": "ID",
                "description": "ID del grupo",
                "type": "int",
            },
        },
    )
    def get(self):
        """Información de un grupo"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("id", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=grupo_namespace)

        try:
            data = get_grupo_from_id(id)
        except:
            grupo_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=grupo_namespace,
            dict_selectable_column="id",
            object_name="grupo",
            xml_root_name=None,
        )


def get_grupos(conditions, params):
    query = f"SELECT {', '.join(columns)} FROM i_grupo g"
    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@grupo_namespace.route("s/")
class Grupos(Resource):
    @grupo_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "nombre": {
                "name": "Nombre",
                "description": "Nombre del grupo",
                "type": "str",
            },
            "acronimo": {
                "name": "Acrónimo",
                "description": "Acrónimo del grupo",
                "type": "str",
            },
            "rama": {
                "name": "Rama",
                "description": "Rama del grupo",
                "type": "str",
            },
            "codigo": {
                "name": "Código",
                "description": "Código del grupo",
                "type": "str",
            },
        },
    )
    def get(self):
        """Búsqueda de grupos"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        nombre = args.get("nombre", None)
        acronimo = args.get("acronimo", None)
        rama = args.get("rama", None)
        codigo = args.get("codigo", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=grupo_namespace)

        conditions = []
        params = []

        if nombre:
            conditions.append(
                "g.nombre COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(nombre)

        if acronimo:
            conditions.append(
                "g.acronimo COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(acronimo)

        if rama:
            conditions.append("g.rama = %s")
            params.append(rama)

        if codigo:
            conditions.append("g.codigo = %s")
            params.append(codigo)

        try:
            data = get_grupos(conditions, params)
        except:
            grupo_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=grupo_namespace,
            dict_selectable_column="id",
            object_name="grupo",
            xml_root_name="grupos",
        )
