from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import (comprobar_api_key)
from utils.timing import func_timer as timer
import utils.pages as pages
import utils.response as response
import utils.date as date
import config.global_config as gconfig

proyecto_namespace = Namespace(
    'proyecto', description="Proyectos (financiación)")

global_responses = gconfig.responses

global_params = gconfig.params

paginate_params = gconfig.paginate_params

columns = [
    "p.id", "p.nombre", "p.tipo", "p.referencia", "p.inicio", "p.fin",
    "DATE_FORMAT(p.inicio,'%d/%m/%Y') as inicio", "DATE_FORMAT(p.fin,'%d/%m/%Y') as fin",
    "p.ambito", "p.concedido", "p.solicitado", "p.prog_financiador", "p.entidad_financiadora",
    "p.competitivo", "p.sisius_id", "DATE_FORMAT(p.creado,'%d/%m/%Y') as creado", "DATE_FORMAT(p.actualizado,'%d/%m/%Y') as actualizado"]

count_prefix = ["COUNT(*) as cantidad"]

left_joins = []


def get_proyecto_from_id(id):
    query = f"SELECT {', '.join(columns)} FROM prisma_proyectos.proyecto p WHERE p.id = %s"
    params = []
    params.append(id)

    db = BaseDatos(database=None)
    result = db.ejecutarConsulta(query, params)

    return result


@proyecto_namespace.route('/')
class Proyecto(Resource):
    @proyecto_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'proyecto': {
                    'name': 'Proyecto',
                    'description': 'ID del proyecto',
                    'type': 'int',
                }, }
    )
    def get(self):
        '''Información de un proyecto'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        id = args.get('proyecto', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=proyecto_namespace)

        try:
            data = get_proyecto_from_id(id)
        except:
            proyecto_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=proyecto_namespace,
                                          dict_selectable_column="id",
                                          object_name="proyecto",
                                          xml_root_name=None,)


def get_proyectos(columns, conditions, params, limit=None, offset=None):
    query = f"SELECT {', '.join(columns)} FROM prisma_proyectos.proyecto p"
    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    if limit is not None and offset is not None:
        query += " LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)

    db = BaseDatos(database=None)
    result = db.ejecutarConsulta(query, params)

    return result


@proyecto_namespace.route('s/')
class Proyectos(Resource):
    @proyecto_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                **paginate_params,
                'referencia': {
                    'name': 'Referencia',
                    'description': 'Código de referencia del proyecto',
                    'type': 'str',
                },
                'nombre': {
                    'name': 'Nombre',
                    'description': 'Nombre del proyecto',
                    'type': 'str',
                },
                'entidad_financiadora': {
                    'name': 'Entidad financiadora',
                    'description': 'Entidad financiadora del proyecto',
                    'type': 'str',
                },
                'desde': {
                    'name': 'Fecha desde',
                    'description': 'Fecha desde la que filtrar proyectos (DD-MM-YYYY)',
                    'type': 'str',
                },
                'hasta': {
                    'name': 'Fecha hasta',
                    'description': 'Fecha desde la que filtrar proyectos (DD-MM-YYYY)',
                    'type': 'str',
                }, }
    )
    def get(self):
        '''Búsqueda de proyectos'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        pagina = int(args.get('pagina', 1))
        longitud_pagina = int(args.get('longitud_pagina', 100))
        referencia = args.get('referencia', None)
        nombre = args.get('nombre', None)
        entidad_financiadora = args.get('entidad_financiadora', None)
        desde = date.str_to_date(args.get('desde', "01-01-1900"))
        hasta = date.str_to_date(
            args.get('hasta', date.get_current_date(format=True)))

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=proyecto_namespace)

        conditions = []
        params = []

        if referencia:
            conditions.append(
                "p.referencia = %s"
            )
            params.append(referencia)

        if nombre:
            conditions.append(
                "p.nombre COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(nombre)

        if entidad_financiadora:
            conditions.append(
                "p.entidad_financiadora COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(entidad_financiadora)

        if desde and hasta:
            conditions.append("p.inicio BETWEEN %s AND %s")
            params.append(desde)
            params.append(hasta)

        amount = int(get_proyectos(
            count_prefix, conditions, params)[1][0])

        longitud_pagina, offset = pages.get_page_offset(
            pagina, longitud_pagina, amount)

        try:
            data = get_proyectos(columns, conditions,
                                 params, longitud_pagina, offset)
        except:
            proyecto_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=proyecto_namespace,
                                          dict_selectable_column="id",
                                          object_name="proyecto",
                                          xml_root_name="proyectos",)


def get_miembros_from_proyecto(id):
    columns = ["pm.id as idMiembro, pm.firma, pm.rol, i.idInvestigador"]
    left_joins = [
        "LEFT JOIN prisma.i_investigador i ON i.idInvestigador = pm.investigador_id"]
    conditions = ["pm.proyecto_id = %s"]
    params = []
    params.append(id)

    query = f"SELECT {', '.join(columns)} FROM prisma_proyectos.proyecto_miembro pm"
    query += f" {' '.join(left_joins)}"
    query += f" WHERE {' '.join(conditions)}"

    db = BaseDatos(database=None)
    result = db.ejecutarConsulta(query, params)

    return result


@proyecto_namespace.route('/miembros/')
class MiembrosProyecto(Resource):
    @proyecto_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'proyecto': {
                    'name': 'Proyecto',
                    'description': 'ID del proyecto',
                    'type': 'int',
                }, }
    )
    def get(self):
        '''Investigadores de una publicación'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        id = args.get('proyecto', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=proyecto_namespace)

        try:
            data = get_miembros_from_proyecto(id)
        except:
            proyecto_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=proyecto_namespace,
                                          dict_selectable_column="idMiembro",
                                          object_name="miembro",
                                          xml_root_name="miembros",)


def get_publicaciones_from_proyecto(id):
    columns = ["pub.idPublicacion as id",
               "pub.tipo", "pub.titulo", "pub.agno as año", "s.titulo as fuente"]
    left_joins = [
        "LEFT JOIN prisma.p_financiacion f ON pub.idPublicacion = f.publicacion_id",
        "LEFT JOIN prisma.p_fuente s ON pub.idFuente = s.idFuente"]
    conditions = ["pub.validado > 1", "pub.eliminado = 0", "pub.tipo != 'Tesis'",
                  "f.codigo IN (SELECT referencia FROM prisma_proyectos.proyecto WHERE id = %s)"]
    params = []
    params.append(id)

    query = f"SELECT {', '.join(columns)} FROM prisma.p_publicacion pub"
    query += f" {' '.join(left_joins)}"
    query += f" WHERE {' AND '.join(conditions)}"

    db = BaseDatos(database=None)
    result = db.ejecutarConsulta(query, params)

    return result


@proyecto_namespace.route('/publicaciones/')
class PublicacionesProyecto(Resource):
    @proyecto_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'proyecto': {
                    'name': 'Proyecto',
                    'description': 'ID del proyecto',
                    'type': 'int',
                }, }
    )
    def get(self):
        '''Investigadores de una publicación'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        id = args.get('proyecto', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=proyecto_namespace)

        try:
            data = get_publicaciones_from_proyecto(id)
        except:
            proyecto_namespace.abort(500, 'Error del servidor')

        # Devolver respuesta

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=proyecto_namespace,
                                          dict_selectable_column="id",
                                          object_name="publicacion",
                                          xml_root_name="publicaciones",)
