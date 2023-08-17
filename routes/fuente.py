from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import (comprobar_api_key)
from utils.timing import func_timer as timer
import utils.pages as pages
import utils.response as response
import utils.date as date_utils
import config.global_config as gconfig

fuente_namespace = Namespace('fuente', description="")

global_responses = gconfig.responses

global_params = gconfig.params

paginate_params = gconfig.paginate_params

columns = ["f.idFuente as id", "f.tipo", "f.titulo", "f.editorial"]

count_columns = ["COUNT(*) as cantidad"]

# OBTENER FUENTE POR ID


def get_fuente_from_id(id):
    query = f"SELECT {', '.join(columns)} FROM p_fuente f" + \
        " WHERE f.idFuente = %s"
    params = []
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@fuente_namespace.route('/')
class Fuente(Resource):
    @fuente_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'fuente': {
                    'name': 'Fuente',
                    'description': 'ID de la fuente',
                    'type': 'int',
                }, }
    )
    def get(self):
        '''Información de una fuente'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        id = args.get('fuente', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=fuente_namespace)

        try:
            data = get_fuente_from_id(id)
        except:
            fuente_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=fuente_namespace,
                                          dict_selectable_column="id",
                                          object_name="fuente",
                                          xml_root_name=None,)

# BUSCADOR DE FUENTES


def get_fuentes(columns, conditions, params, limit=None, offset=None):
    query = f"SELECT {', '.join(columns)} FROM p_fuente f WHERE {' AND '.join(conditions)}"

    if limit is not None and offset is not None:
        query += " LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@fuente_namespace.route('s/')
class Fuentes(Resource):
    @fuente_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                **paginate_params,
                'titulo': {
                    'name': 'Título',
                    'description': 'Título de la fuente',
                    'type': 'int',
                },
                'identificador': {
                    'name': 'Identificador',
                    'description': 'Identificador de la fuente (ISSN, eISSN, ISBN, eISBN, DOI...)',
                    'type': 'int',
                },
                'editorial': {
                    'name': 'Título',
                    'description': 'Título de la fuente',
                    'type': 'int',
                }, }
    )
    def get(self):
        '''Búsqueda de fuentes'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        pagina = int(args.get('pagina', 1))
        longitud_pagina = int(args.get('longitud_pagina', 100))
        titulo = args.get('titulo', None)
        identificador = args.get('identificador', None)
        editorial = args.get('editorial', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=fuente_namespace)

        conditions = []
        params = []

        if titulo:
            conditions.append(
                "f.titulo COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')")
            params.append(titulo)
        if identificador:
            conditions.append(
                "f.idFuente IN (SELECT idf.idFuente FROM p_identificador_fuente idf WHERE idf.valor = %s)")
            params.append(identificador)
        if editorial:
            conditions.append(
                "f.editorial COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')")
            params.append(editorial)

        amount = int(get_fuentes(count_columns, conditions, params)[1][0])

        limit, offset = pages.get_page_offset(
            pagina, longitud_pagina, amount)

        try:
            data = get_fuentes(columns, conditions, params,
                               limit, offset)
        except:
            fuente_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=fuente_namespace,
                                          dict_selectable_column="id",
                                          object_name="fuente",
                                          xml_root_name="fuentes")

# OBTENER IDENTIFICADORES DE UNA FUENTE


def get_identificadores_from_fuente(id):
    columns = ["idf.idIdentificador as id, idf.tipo, idf.valor"]

    query = f"SELECT {', '.join(columns)} FROM p_identificador_fuente idf " + \
        " WHERE idf.idFuente = %s"
    params = []
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@fuente_namespace.route('/identificadores/')
class IdentificadoresFuente(Resource):
    @fuente_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'fuente': {
                    'name': 'Fuente',
                    'description': 'ID de la fuente',
                    'type': 'int',
                }, }
    )
    def get(self):
        '''Identificadores de una fuente (ISSN, eISSN, ISBN, eISBN, DOI...)'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        id = args.get('fuente', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=fuente_namespace)

        try:
            data = get_identificadores_from_fuente(id)
        except:
            fuente_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=fuente_namespace,
                                          dict_selectable_column="id",
                                          object_name="identificador",
                                          xml_root_name="identificadores",)

# OBTENER PUBLICACIONES DE UNA FUENTE


@fuente_namespace.route('/publicaciones/')
class PublicacionesFuente(Resource):
    @fuente_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'estadisticas': {
                    'name': 'Estadísticas',
                    'description': 'Si se activa, se devolverá un resumen de estadísticas de la búsqueda',
                    'type': 'bool',
                    'enum': ["True", "False"],
                },
                'fuente': {
                    'name': 'Fuente',
                    'description': 'ID de la fuente',
                    'type': 'int',
                },
                'coleccion': {
                    'name': 'Colección',
                            'description': 'ID de la colección por la que filtrar',
                            'type': 'int',
                }, }
    )
    def get(self):
        '''Publicaciones de una fuente'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)

        estadisticas = True if (args.get('estadisticas', "False").lower()
                                == "true") else False
        id = args.get('fuente', None)
        coleccion = args.get('coleccion', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=fuente_namespace)

        request_url = f'http://{request.host}/'
        request_urn = f'publicaciones/?salida={accept_type}&api_key={api_key}&fuente={response.empty_string_if_none(id)}' + \
            f'&longitud_pagina=0&estadisticas={estadisticas}&coleccion={response.empty_string_if_none(coleccion)}'
        referrer = request.referrer
        return response.generate_response_from_uri(request_url, request_urn, referrer)
