from flask import request, Response
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import (comprobar_api_key)
from utils.timing import func_timer as timer
import utils.format as format
import utils.pages as pages
import utils.response as response
import config.global_config as gconfig


publicacion_namespace = Namespace(
    'publicacion', description="")

global_responses = gconfig.responses

global_params = gconfig.params
global_params = {**global_params,
                 'inactivos': {
                     'name': 'ID',
                     'description': 'Incluir publicaciones eliminadas',
                     'type': 'bool',
                     'enum': ["True", "False"],
                 }, }
# COLUMNAS DEVUELTAS EN LAS CONSULTAS DE PUBLICACION

columns = ["p.idPublicacion as id", "p.titulo", "p.agno as año",
           "fuente.tipo as fuente_tipo", "fuente.titulo as fuente_titulo", "fuente.editorial as fuente_editorial"]

left_joins = [" LEFT JOIN p_fuente fuente ON fuente.idFuente = p.idFuente"]

# Prefijos para agrupar items en la consulta

nested = {"fuente": "fuente"}


def merge_query(columns: list[str], left_joins: list[str]):
    result = f"SELECT {', '.join(columns)} " + "FROM p_publicacion p"
    result += " ".join(left_joins)

    return result


def get_publicacion_from_id(columns: list[str], left_joins: list[str], inactivos: bool, id: int):
    query = merge_query(columns, left_joins)

    params = []
    query += " WHERE p.idPublicacion = %s"
    params.append(id)

    if not inactivos:
        query += " AND p.eliminado = 0"

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@publicacion_namespace.route('/')
class Publicacion(Resource):
    @publicacion_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'id': {
                    'name': 'ID',
                            'description': 'ID de la publicación',
                            'type': 'int',
                }, }
    )
    def get(self):
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        id = args.get('id', None)
        inactivos = True if (args.get('inactivos', "False").lower()
                             == "true") else False

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=publicacion_namespace)

        try:
            data = get_publicacion_from_id(columns, left_joins, inactivos, id)
        except:
            publicacion_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested=nested,
                                          namespace=publicacion_namespace,
                                          dict_selectable_column="id",
                                          object_name="publicacion",
                                          xml_root_name="",)
