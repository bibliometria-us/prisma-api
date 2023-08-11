from flask import request, Response
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import (comprobar_api_key)
from utils.timing import func_timer as timer
import utils.format as format
import utils.pages as pages
import utils.response as response
import utils.date as date_utils
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
paginate_params = gconfig.paginate_params
# COLUMNAS DEVUELTAS EN LAS CONSULTAS DE PUBLICACION

columns = ["p.idPublicacion as id", "p.titulo", "p.tipo as tipo", "p.agno as año",
           "fuente.tipo as fuente_tipo", "fuente.titulo as fuente_titulo", "fuente.editorial as fuente_editorial",
           "dialnet.valor as identificador_dialnet", "doi.valor as identificador_doi", "idus.valor as identificador_idus",
           "pubmed.valor as identificador_pubmed", "scopus.valor as identificador_scopus", "wos.valor as identificador_wos",]

count_prefix = ["COUNT(*) as cantidad"]

# Plantilla para left joins de identificadores de publicación
plantilla_ids = " LEFT JOIN (SELECT idPublicacion, valor FROM p_identificador_publicacion WHERE tipo = '{0}') as {0} ON {0}.idPublicacion = p.idPublicacion"


left_joins = [plantilla_ids.format("dialnet"),
              plantilla_ids.format("doi"),
              plantilla_ids.format("idus"),
              plantilla_ids.format("pubmed"),
              plantilla_ids.format("scopus"),
              plantilla_ids.format("wos"),
              " LEFT JOIN p_fuente fuente ON fuente.idFuente = p.idFuente"]

# Prefijos para agrupar items en la consulta

nested = {"fuente": "fuente",
          "identificador": "identificadores"}


def get_publicacion_from_id(columns: list[str], left_joins: list[str], inactivos: bool, id: int):
    query = f"SELECT {', '.join(columns)} " + "FROM p_publicacion p"
    query += " ".join(left_joins)

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
                                          xml_root_name=None,)


@timer
def get_publicaciones(columns, left_joins, inactivos, conditions, params, limit=None, offset=None):
    query = f"SELECT {', '.join(columns)} " + "FROM p_publicacion p"
    query += " ".join(left_joins)

    if inactivos:
        query += " WHERE p.eliminado IN (0,1)"
    else:
        query += " WHERE p.eliminado = 0"

    query += " AND p.tipo != 'tesis'"
    query += f" AND ({' AND '.join(conditions)})"
    query += " ORDER BY p.agno DESC, p.titulo ASC"

    if limit is not None and offset is not None:
        query += " LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result

# Calcula las estadísticas de una búsqueda de publicaciones


def get_estadisticas_publicaciones(columns, left_joins, inactivos, conditions, params):
    table = get_publicaciones(
        columns, left_joins, inactivos, conditions, params)

    # Total de publicaciones
    total_rows = len(table) - 1

    # Almacenar índices
    indexes = {column: index for index, column in enumerate(
        table[0]) if column == 'tipo' or column.startswith('identificador_')}

    # Función para insertar contadores en diccionario

    def count_in_dict(dict: dict, key: str):
        if key not in dict:
            dict[key] = 1
        else:
            dict[key] += 1
    # Recorrer tabla para contar
    result_dict = {"Total": total_rows}
    for row in table[1:]:
        for index in indexes:
            value = row[indexes[index]]
            if index == "tipo":
                count_in_dict(result_dict, value)
            if index.startswith("identificador_"):
                if value is not None:
                    count_in_dict(result_dict, index)

    result = [("tipo", "valor")]

    for res in result_dict:
        result.append((res, result_dict[res]))

    return result


@publicacion_namespace.route('es/')
class Publicaciones(Resource):
    @publicacion_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                **paginate_params,
                'estadisticas': {
                    'name': 'Estadísticas',
                            'description': 'Si se activa, se devolverá un resumen de estadísticas de la búsqueda',
                            'type': 'bool',
                            'enum': ["True", "False"],
                },
                'investigador': {
                    'name': 'Investigador',
                            'description': 'ID de investigador',
                            'type': 'int',
                },
                'titulo': {
                    'name': 'Título',
                            'description': 'Título de la publicación',
                            'type': 'int',
                },

                'comienzo': {
                    'name': 'Año de comienzo',
                            'description': 'Año a partir del cual buscar las publicaciones (inclusive)',
                            'type': 'int',
                },

                'fin': {
                    'name': 'Año de fin',
                            'description': 'Año hasta el cual buscar las publicaciones (inclusive)',
                            'type': 'int',
                },
                'departamento': {
                    'name': 'Departamento',
                            'description': 'ID del departamento por el que filtrar',
                            'type': 'int',
                }, }
    )
    def get(self):
        headers = request.headers
        args = request.args

        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        pagina = int(args.get('pagina', 1))
        longitud_pagina = int(args.get('longitud_pagina', 100))
        total_elementos = args.get('total_elementos', None)
        api_key = args.get('api_key', None)
        investigador = args.get('investigador', None)
        titulo = args.get('titulo', None)
        inactivos = True if (args.get('inactivos', "False").lower()
                             == "true") else False
        estadisticas = True if (args.get('estadisticas', "False").lower()
                                == "true") else False
        comienzo = int(args.get('comienzo', 1900))
        fin = int(args.get('fin', date_utils.get_current_year()))
        departamento = args.get('departamento', None)
        comprobar_api_key(api_key=api_key, namespace=publicacion_namespace)

        # Cargar condiciones de búsqueda

        conditions = []
        params = []

        if investigador:
            conditions.append(
                "p.idPublicacion in (SELECT idPublicacion from p_autor WHERE idInvestigador = %s)")
            params.append(investigador)
        if titulo:
            conditions.append(
                "p.titulo COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(titulo)
        if comienzo and fin:
            conditions.append("p.agno BETWEEN %s AND %s")
            params.append(comienzo)
            params.append(fin)
        if departamento:
            conditions.append(
                "p.idPublicacion IN (SELECT a.idPublicacion FROM p_autor a LEFT JOIN i_investigador i ON i.idInvestigador = a.idInvestigador WHERE i.idDepartamento = %s)")
            params.append(departamento)

        # Parámetros de paginación
        is_paginable = not estadisticas

        offset = None

        if is_paginable:
            if not total_elementos:
                amount = int(get_publicaciones(
                    count_prefix, left_joins, inactivos, conditions, params)[1][0])
            else:
                amount = int(total_elementos)

            longitud_pagina, offset = pages.get_page_offset(
                pagina, longitud_pagina, amount)

        try:
            # Consulta normal de investigadores
            if not estadisticas:
                data = get_publicaciones(
                    columns, left_joins, inactivos, conditions, params, longitud_pagina, offset)
                dict_selectable_column = "id"
                object_name = "publicacion"
                xml_root_name = "publicaciones"
            # Consulta de estadísticas de la búsqueda
            else:
                data = get_estadisticas_publicaciones(
                    columns, left_joins, inactivos, conditions, params)
                dict_selectable_column = "tipo"
                object_name = "estadistica"
                xml_root_name = "estadisticas"
        except:
            publicacion_namespace.abort(500, 'Error del servidor')

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested=nested,
                                          namespace=publicacion_namespace,
                                          dict_selectable_column=dict_selectable_column,
                                          object_name=object_name,
                                          xml_root_name=xml_root_name,)
