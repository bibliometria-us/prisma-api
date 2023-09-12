from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import (comprobar_api_key)
from utils.timing import func_timer as timer
import utils.pages as pages
import utils.response as response
import utils.date as date_utils
import config.global_config as gconfig


publicacion_namespace = Namespace(
    'publicacion', description="Publicaciones")

global_responses = gconfig.responses

global_params = gconfig.params

paginate_params = gconfig.paginate_params
# COLUMNAS DEVUELTAS EN LAS CONSULTAS DE PUBLICACION

columns = ["p.idPublicacion as id", "p.titulo", "p.tipo as tipo", "p.agno as año",
           "fuente.tipo as fuente_tipo", "fuente.titulo as fuente_titulo", "fuente.editorial as fuente_editorial",
           "dialnet.valor as identificador_dialnet", "doi.valor as identificador_doi", "idus.valor as identificador_idus",
           "pubmed.valor as identificador_pubmed", "scopus.valor as identificador_scopus", "wos.valor as identificador_wos",
           "cod_programa.valor as datos_cod_programa", "congreso.valor as datos_congreso", "doceuropeo.valor as datos_doceuropeo",
           "fecha_lectura.valor as datos_fecha_lectura", "internacional.valor as datos_internacional",
           "lugar_lectura.valor as datos_lugar_lectura", "cod_programa.valor as datos_cod_programa",
           "nota.valor as datos_nota", "num_articulo.valor as datos_num_articulo", "numero.valor as datos_numero", ]

count_prefix = ["COUNT(*) as cantidad"]

# Plantilla para left joins de identificadores de publicación
plantilla_ids = " LEFT JOIN (SELECT idPublicacion, valor FROM p_identificador_publicacion WHERE tipo = '{0}') as {0} ON {0}.idPublicacion = p.idPublicacion"
plantilla_datos = " LEFT JOIN (SELECT idPublicacion, valor FROM p_dato_publicacion dp WHERE tipo = '{0}') as {0} ON {0}.idPublicacion = p.idPublicacion"

left_joins = [plantilla_ids.format("dialnet"), plantilla_ids.format("doi"), plantilla_ids.format("idus"), plantilla_ids.format("pubmed"), plantilla_ids.format("scopus"), plantilla_ids.format("wos"),
              plantilla_datos.format("cod_programa"), plantilla_datos.format(
                  "congreso"), plantilla_datos.format("doceuropeo"),
              plantilla_datos.format("fecha_lectura"), plantilla_datos.format(
                  "internacional"), plantilla_datos.format("lugar_lectura"),
              plantilla_datos.format("volumen"), plantilla_datos.format(
                  "nota"), plantilla_datos.format("num_articulo"),
              plantilla_datos.format("numero"), plantilla_datos.format(
                  "pag_fin"), plantilla_datos.format("pag_inicio"),
              plantilla_datos.format("titulo_alt"),
              " LEFT JOIN p_fuente fuente ON fuente.idFuente = p.idFuente"]

# Prefijos para agrupar items en la consulta

nested = {"fuente": "fuente",
          "identificador": "identificadores",
          "autor": "autor",
          "datos": "datos"}


def get_publicacion_from_id(columns: list[str], left_joins: list[str], id: int):
    query = f"SELECT {', '.join(columns)} " + "FROM p_publicacion p"
    query += " ".join(left_joins)

    params = []
    query += " WHERE p.idPublicacion = %s"
    params.append(id)

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
        '''Información de una publicación'''
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        id = args.get('id', None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=publicacion_namespace)

        try:
            data = get_publicacion_from_id(columns, left_joins, id)
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
                'total_elementos': {
                    'name': 'Total de elementos',
                    'description': 'Total de elementos en la búsqueda. De ser conocido, introducirlo en la consulta para mejorar la eficiencia de la paginación',
                    'type': 'int',
                },
                'inactivos': {
                    'name': 'inactivos',
                    'description': 'Incluir publicaciones eliminadas',
                    'type': 'bool',
                    'enum': ["True", "False"],
                },
                'estadisticas': {
                    'name': 'Estadísticas',
                            'description': 'Si se activa, se devolverá un resumen de estadísticas de la búsqueda',
                            'type': 'bool',
                            'enum': ["True", "False"],
                },
                'tesis': {
                    'name': 'Tesis',
                    'description': 'Si está activo, se devolverán las tesis asociadas a los filtros de búsqueda',
                    'type': 'bool',
                    'enum': ["True", "False"],
                    'default': "False"
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
                },
                'grupo': {
                    'name': 'Grupo',
                            'description': 'ID del grupo de investigación por el que filtrar',
                            'type': 'int',
                },
                'instituto': {
                    'name': 'Instituto',
                            'description': 'ID del instituto por el que filtrar',
                            'type': 'int',
                },
                'doctorado': {
                    'name': 'Programa de doctorado',
                            'description': 'ID del programa de doctorado por el que filtrar',
                            'type': 'int',
                },
                'fuente': {
                    'name': 'Fuente',
                            'description': 'ID de la fuente de la publicación por la que filtrar',
                            'type': 'int',
                },
                'coleccion': {
                    'name': 'Colección',
                            'description': 'ID de la colección por la que filtrar',
                            'type': 'int',
                },
                'editorial': {
                    'name': 'Editorial',
                            'description': 'ID de la editorial por la que filtrar',
                            'type': 'int',
                }, }
    )
    def get(self):
        '''Búsqueda de publicaciones'''
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
        tesis = True if (args.get('tesis', "False").lower()
                         == "true") else False
        comienzo = int(args.get('comienzo', 1900))
        fin = int(args.get('fin', date_utils.get_current_year()))
        departamento = args.get('departamento', None)
        grupo = args.get('grupo', None)
        instituto = args.get('instituto', None)
        doctorado = args.get('doctorado', None)
        fuente = args.get('fuente', None)
        coleccion = args.get('coleccion', None)
        editorial = args.get('editorial', None)

        comprobar_api_key(api_key=api_key, namespace=publicacion_namespace)

        # Cargar condiciones de búsqueda

        _left_joins = left_joins.copy()
        _columns = columns.copy()
        conditions = []
        params = []

        if investigador:
            conditions.append(
                "p.idPublicacion in (SELECT idPublicacion from p_autor WHERE idInvestigador = %s)")
            _left_joins.extend(
                [" LEFT JOIN p_autor autor ON autor.idPublicacion = p.idPublicacion AND autor.idInvestigador = %s",
                 ])
            _columns.extend(
                ["autor.rol as autor_rol", "autor.orden as autor_orden",
                 "(SELECT MAX(a.orden) FROM p_autor a WHERE a.idPublicacion = p.idPublicacion) AS autor_max_orden"])
            params.append(investigador)
            params.append(investigador)
        if tesis:
            conditions.append("p.tipo = 'Tesis'")
        else:
            conditions.append("p.tipo != 'Tesis'")
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
                "p.idPublicacion IN (SELECT a.idPublicacion FROM p_autor a LEFT JOIN i_investigador_activo i ON i.idInvestigador = a.idInvestigador WHERE i.idDepartamento = %s)")
            params.append(departamento)
        if grupo:
            conditions.append(
                "p.idPublicacion IN (SELECT a.idPublicacion FROM p_autor a LEFT JOIN i_investigador_activo i ON i.idInvestigador = a.idInvestigador WHERE i.idGrupo = %s)")
            params.append(grupo)
        if instituto:
            conditions.append(
                "p.idPublicacion IN (SELECT a.idPublicacion FROM p_autor a WHERE a.idInvestigador in (SELECT mi.idInvestigador FROM i_miembro_instituto mi WHERE mi.idInstituto = %s))")
            params.append(instituto)
        if doctorado:
            conditions.append(
                "p.idPublicacion IN (SELECT a.idPublicacion FROM p_autor a WHERE a.idInvestigador in (SELECT pd.idInvestigador FROM i_profesor_doctorado pd WHERE pd.idDoctorado = %s))")
            params.append(doctorado)
        if fuente:
            conditions.append(
                "p.idFuente = %s "
            )
            params.append(fuente)
        if coleccion:
            conditions.append(
                "p.idFuente IN (SELECT df.idFuente FROM p_dato_fuente df WHERE df.valor = %s AND df.tipo = 'coleccion')")
            params.append(coleccion)
        if editorial:
            conditions.append(
                "p.idFuente IN (SELECT f.idFuente FROM p_fuente f WHERE f.editorial IN (SELECT e.nombre FROM p_editor e WHERE e.id = %s))")
            params.append(editorial)

        # Parámetros de paginación
        is_paginable = not estadisticas

        offset = None

        if is_paginable:
            if not total_elementos:
                amount = int(get_publicaciones(
                    count_prefix, _left_joins, inactivos, conditions, params)[1][0])
            else:
                amount = int(total_elementos)

            longitud_pagina, offset = pages.get_page_offset(
                pagina, longitud_pagina, amount)

        try:
            # Consulta normal de investigadores
            if not estadisticas:
                data = get_publicaciones(
                    _columns, _left_joins, inactivos, conditions, params, limit=longitud_pagina, offset=offset)
                dict_selectable_column = "id"
                object_name = "publicacion"
                xml_root_name = "publicaciones"
            # Consulta de estadísticas de la búsqueda
            else:
                data = get_estadisticas_publicaciones(
                    columns, _left_joins, inactivos, conditions, params)
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


# AUTORES DE PUBLICACIÓN

def get_autores_from_publicacion(id_publicacion: int):
    params = []
    query = "SELECT a.idAutor, a.idInvestigador, a.firma, a.rol, a.orden FROM p_autor a WHERE a.idPublicacion = %s ORDER BY rol ASC, orden ASC"

    params.append(id_publicacion)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@publicacion_namespace.route('/autores/')
class AutoresPublicacion(Resource):
    @publicacion_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'publicacion': {
                    'name': 'Publicación',
                    'description': 'ID de la publicación',
                    'type': 'int',
                }, })
    def get(self):
        '''Autores de la publicación'''
        headers = request.headers
        args = request.args

        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        publicacion = args.get('publicacion', None)

        comprobar_api_key(api_key=api_key, namespace=publicacion_namespace)

        try:
            data = get_autores_from_publicacion(publicacion)
        except:
            publicacion_namespace.abort(500, 'Error del servidor')

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested=nested,
                                          namespace=publicacion_namespace,
                                          dict_selectable_column="idAutor",
                                          object_name="autor",
                                          xml_root_name="autores",)

# DATOS DE PUBLICACIÓN


def get_datos_from_publicacion(id_publicacion: int):
    params = []
    query = "SELECT dp.tipo, dp.valor FROM p_dato_publicacion dp WHERE dp.idPublicacion = %s"

    params.append(id_publicacion)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@publicacion_namespace.route('/datos/')
class DatosPublicacion(Resource):
    @publicacion_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'publicacion': {
                    'name': 'Publicación',
                    'description': 'ID de la publicación',
                    'type': 'int',
                }, })
    def get(self):
        '''Datos de la publicación, como número de páginas, página de inicio/fin, nota, volumen, congreso...'''
        headers = request.headers
        args = request.args

        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        publicacion = args.get('publicacion', None)

        comprobar_api_key(api_key=api_key, namespace=publicacion_namespace)

        try:
            data = get_datos_from_publicacion(publicacion)
        except:
            publicacion_namespace.abort(500, 'Error del servidor')

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested=nested,
                                          namespace=publicacion_namespace,
                                          dict_selectable_column="tipo",
                                          object_name="dato",
                                          xml_root_name="datos",)

# OPEN ACCESS PUBLICACIÓN


def get_acceso_abierto_from_publicacion(id_publicacion: int):
    params = []
    query = "SELECT aa.id, aa.valor as via, aa.origen FROM p_acceso_abierto aa WHERE aa.publicacion_id = %s"

    params.append(id_publicacion)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@publicacion_namespace.route('/acceso_abierto/')
class AccesoAbiertoPublicacion(Resource):
    @publicacion_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'publicacion': {
                    'name': 'Publicación',
                    'description': 'ID de la publicación',
                    'type': 'int',
                }, })
    def get(self):
        '''Datos de acceso abierto de una publicación'''
        headers = request.headers
        args = request.args

        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        publicacion = args.get('publicacion', None)

        comprobar_api_key(api_key=api_key, namespace=publicacion_namespace)

        try:
            data = get_acceso_abierto_from_publicacion(publicacion)
        except:
            publicacion_namespace.abort(500, 'Error del servidor')

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested=nested,
                                          namespace=publicacion_namespace,
                                          dict_selectable_column="id",
                                          object_name="acceso_abierto",
                                          xml_root_name="acceso_abiertos",)

# MÉTRICAS DE PUBLICACIÓN


def get_metricas_from_publicacion(id_publicacion: int):
    params = []
    query = "SELECT m.idMetrica as id, m.metrica, m.basedatos, m.valor, DATE_FORMAT(m.fechaActualizacion,'%d/%m/%Y') as fecha_actualizacion FROM m_publicaciones m WHERE m.idPublicacion = %s"

    params.append(id_publicacion)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@publicacion_namespace.route('/metricas/')
class MetricasPublicacion(Resource):
    @publicacion_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'publicacion': {
                    'name': 'Publicación',
                    'description': 'ID de la publicación',
                    'type': 'int',
                },
                })
    def get(self):
        '''Métricas de la publicación: citas de la publicación y Premio Extraordinario de Doctorado US'''
        headers = request.headers
        args = request.args

        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = args.get('api_key', None)
        publicacion = args.get('publicacion', None)

        comprobar_api_key(api_key=api_key, namespace=publicacion_namespace)

        try:
            data = get_metricas_from_publicacion(publicacion)
        except:
            publicacion_namespace.abort(500, 'Error del servidor')

        return response.generate_response(data=data,
                                          output_types=["json", "xml", "csv"],
                                          accept_type=accept_type,
                                          nested=nested,
                                          namespace=publicacion_namespace,
                                          dict_selectable_column="id",
                                          object_name="metrica",
                                          xml_root_name="metricas",)
