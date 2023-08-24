from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import (comprobar_api_key)
from utils.timing import func_timer as timer
import utils.response as response
import config.global_config as gconfig

doctorado_namespace = Namespace(
    'doctorado', description="Programas de doctorado")

global_responses = gconfig.responses

global_params = gconfig.params

columns = ["d.idDoctorado as id", "d.nombre"]


def get_doctorado_from_id(id):
    query = f"SELECT {', '.join(columns)} FROM i_doctorado d WHERE d.idDoctorado = %s"
    params = []
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@doctorado_namespace.route('/')
class Doctorado(Resource):
    @doctorado_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'id': {
                    'name': 'ID',
                    'description': 'ID del doctorado',
                    'type': 'int',
                }, }
    )
    def get(self):
        '''Información de un programa de doctorado'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        id = args.get('id', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=doctorado_namespace)

        try:
            data = get_doctorado_from_id(id)
        except:
            doctorado_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=doctorado_namespace,
                                          dict_selectable_column="id",
                                          object_name="doctorado",
                                          xml_root_name=None,)


def get_doctorados(conditions, params):
    query = f"SELECT {', '.join(columns)} FROM i_doctorado d"
    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@doctorado_namespace.route('s/')
class Doctorados(Resource):
    @doctorado_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'nombre': {
                    'name': 'Nombre',
                    'description': 'Nombre del doctorado',
                    'type': 'str',
                },
                }
    )
    def get(self):
        '''Búsqueda de programas de doctorado'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        nombre = args.get('nombre', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=doctorado_namespace)

        conditions = []
        params = []

        if nombre:
            conditions.append(
                "d.nombre COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')")
            params.append(nombre)

        try:
            data = get_doctorados(conditions, params)
        except:
            doctorado_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=doctorado_namespace,
                                          dict_selectable_column="id",
                                          object_name="doctorado",
                                          xml_root_name="doctorados",)
