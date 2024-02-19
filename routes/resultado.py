from flask import request
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import comprobar_api_key
from utils.timing import func_timer as timer
import utils.pages as pages
import utils.response as response
import config.global_config as gconfig

resultado_namespace = Namespace("resultado", description="Resultados de investigación")

global_responses = gconfig.responses

global_params = gconfig.params

paginate_params = gconfig.paginate_params

nested = {"datos": "datos"}

columns = [
    "p.id",
    "p.tipo",
    "p.numero_solicitud",
    "DATE_FORMAT(p.fecha_solicitud,'%d/%m/%Y') as fecha_solicitud",
    "p.titulo",
    "p.idioma_titulo",
    "p.resumen",
    "p.visible",
    "url_invenes.valor as datos_valor",
    "otros_codigos.valor as datos_otros_codigos",
    "url_archivo.valor as datos_url_archivo",
    "fecha_concesion.valor as datos_fecha_concesion",
    "fecha_publicacion_so.valor as datos_fecha_publicacion_so",
    "numero_publicacion.valor as datos_numero_publicacion",
    "fecha_publicacion.valor as datos_fecha_publicacion",
    "titulo_alt.valor as datos_titulo_alt",
    "enlace.valor as datos_enlace",
    "url.valor as datos_url",
]

count_columns = ["COUNT(*) as cantidad"]

plantilla_datos = " LEFT JOIN (SELECT patente_id, valor FROM dato_patente dp WHERE dp.tipo = '{0}') as {0} ON {0}.patente_id = p.id"
left_joins = [
    plantilla_datos.format("url_invenes"),
    plantilla_datos.format("otros_codigos"),
    plantilla_datos.format("url_archivo"),
    plantilla_datos.format("fecha_concesion"),
    plantilla_datos.format("fecha_publicacion_so"),
    plantilla_datos.format("numero_publicacion"),
    plantilla_datos.format("fecha_publicacion"),
    plantilla_datos.format("titulo_alt"),
    plantilla_datos.format("enlace"),
    plantilla_datos.format("url"),
]


def get_resultado_from_id(id):
    query = f"SELECT {', '.join(columns)} FROM patente p"
    query += f" {' '.join(left_joins)}"
    query += " WHERE p.id = %s"

    params = []
    params.append(id)

    db = BaseDatos(database="prisma_resultado")
    result = db.ejecutarConsulta(query, params)

    return result


@resultado_namespace.route("/")
class Patente(Resource):
    @resultado_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "patente": {
                "name": "Patente",
                "description": "ID de la patente",
                "type": "int",
            },
        },
    )
    def get(self):
        """Información de una patente"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("patente", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=resultado_namespace)

        try:
            data = get_resultado_from_id(id)
        except:
            resultado_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested=nested,
            namespace=resultado_namespace,
            dict_selectable_column="id",
            object_name="patente",
            xml_root_name=None,
        )


def get_resultados(columns, conditions, params, limit=None, offset=None):
    query = f"SELECT {', '.join(columns)} FROM prisma_resultado.patente p"

    left_joins_str = f" {' '.join(left_joins)}"
    left_joins_str = left_joins_str.replace(
        "dato_patente", "prisma_resultado.dato_patente"
    )
    query += left_joins_str

    query += f" WHERE {' AND '.join(conditions)}"

    if limit is not None and offset is not None:
        query += " LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)

    db = BaseDatos(database=None)
    result = db.ejecutarConsulta(query, params)

    return result


@resultado_namespace.route("s/")
class Patentes(Resource):
    @resultado_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            **paginate_params,
            "activos": {
                "name": "Activos",
                "description": "Mostrar solo resultados activos",
                "type": "bool",
                "enum": "",
            },
            "titulo": {
                "name": "Título",
                "description": "Título del resultado",
                "type": "str",
            },
            "codigo": {
                "name": "Código",
                "description": "Número de solicitud/concesión",
                "type": "str",
            },
            "año_concesion": {
                "name": "Año de concesión",
                "description": "Año de concesión",
                "type": "int",
            },
            "materia_cip": {
                "name": "Materia CIP",
                "description": "Código de la materia CIP por la que filtrar",
                "type": "str",
            },
            "departamento": {
                "name": "Departamento",
                "description": "ID del departamento por el que filtrar",
                "type": "str",
            },
            "grupo": {
                "name": "Grupo",
                "description": "ID del grupo de investigación por el que filtrar",
                "type": "str",
            },
            "instituto": {
                "name": "Instituto",
                "description": "ID del instituto de investigación por el que filtrar",
                "type": "int",
            },
            "doctorado": {
                "name": "Programa de doctorado",
                "description": "ID del programa de doctorado por el que filtrar",
                "type": "int",
            },
        },
    )
    def get(self):
        """Búsqueda de patentes"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        pagina = int(args.get("pagina", 1))
        longitud_pagina = int(args.get("longitud_pagina", 100))
        activos = True if (args.get("activos", "True").lower() == "true") else False
        titulo = args.get("titulo", None)
        codigo = args.get("codigo", None)
        año_concesion = args.get("año_concesion", None)
        materia_cip = args.get("materia_cip", None)
        departamento = args.get("departamento", None)
        grupo = args.get("grupo", None)
        instituto = args.get("instituto", None)
        doctorado = args.get("doctorado", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=resultado_namespace)

        conditions = []
        params = []

        if activos:
            conditions.append("p.visible = 1")
        if titulo:
            conditions.append(
                "p.titulo COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(titulo)
        if codigo:
            conditions.append(
                "%s IN (p.numero_solicitud, (SELECT dp.valor FROM prisma_resultado.dato_patente dp WHERE dp.tipo = 'numero_publicacion' AND dp.patente_id = p.id))"
            )
            params.append(codigo)
        if año_concesion:
            conditions.append(
                "p.id IN (SELECT dp.patente_id FROM prisma_resultado.dato_patente dp WHERE dp.tipo = 'fecha_concesion' AND LEFT(dp.valor, 4) = %s)"
            )
            params.append(año_concesion)
        if materia_cip:
            conditions.append(
                "p.id IN (SELECT mp.patente_id FROM prisma_resultado.materia_patente mp WHERE mp.clasificacion = 'cip' AND LEFT(mp.materia, 1) = %s)"
            )
            params.append(materia_cip)
        if departamento:
            conditions.append(
                "p.id IN (SELECT ip.patente_id FROM prisma_resultado.inventor_patente ip WHERE ip.investigador_id IN (SELECT i.idInvestigador FROM prisma.i_investigador_activo i WHERE idDepartamento = %s))"
            )
            params.append(departamento)
        if grupo:
            conditions.append(
                "p.id IN (SELECT ip.patente_id FROM prisma_resultado.inventor_patente ip WHERE ip.investigador_id IN (SELECT i.idInvestigador FROM prisma.i_investigador_activo i WHERE idGrupo = %s))"
            )
            params.append(grupo)
        if instituto:
            conditions.append(
                "p.id IN (SELECT ip.patente_id FROM prisma_resultado.inventor_patente ip WHERE ip.investigador_id IN (SELECT i.idInvestigador FROM prisma.i_investigador_activo i WHERE i.idInvestigador IN (SELECT ims.idInvestigador FROM prisma.i_miembro_instituto ims WHERE ims.idInstituto = %s)))"
            )
            params.append(instituto)
        if doctorado:
            conditions.append(
                "p.id IN (SELECT ip.patente_id FROM prisma_resultado.inventor_patente ip WHERE ip.investigador_id IN (SELECT i.idInvestigador FROM prisma.i_investigador_activo i WHERE i.idInvestigador IN (SELECT ipd.idInvestigador FROM prisma.i_profesor_doctorado ipd WHERE ipd.idDoctorado = %s)))"
            )
            params.append(doctorado)

        amount = int(get_resultados(count_columns, conditions, params)[1][0])

        longitud_pagina, offset = pages.get_page_offset(pagina, longitud_pagina, amount)
        try:
            data = get_resultados(
                columns, conditions, params, limit=longitud_pagina, offset=offset
            )
        except:
            resultado_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested=nested,
            namespace=resultado_namespace,
            dict_selectable_column="id",
            object_name="patente",
            xml_root_name="patentes",
        )


def get_materias_from_resultado(id):
    columns = [
        "mp.id",
        "mp.clasificacion",
        "mp.materia",
        "mp.edicion",
        "mcip.materia as titulo",
    ]
    left_joins = [
        " LEFT JOIN materia_cip mcip ON LEFT(mp.materia, 1) COLLATE utf8mb3_general_ci = mcip.codigo"
    ]
    query = f"SELECT {', '.join(columns)} FROM materia_patente mp"
    query += f" {' '.join(left_joins)}"
    query += " WHERE mp.patente_id = %s"

    params = []
    params.append(id)

    db = BaseDatos(database="prisma_resultado")
    result = db.ejecutarConsulta(query, params)

    return result


@resultado_namespace.route("/materias/")
class MateriasPatente(Resource):
    @resultado_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "patente": {
                "name": "Patente",
                "description": "ID de la patente",
                "type": "int",
            },
        },
    )
    def get(self):
        """Materias de una patente"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("patente", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=resultado_namespace)

        try:
            data = get_materias_from_resultado(id)
        except:
            resultado_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested=nested,
            namespace=resultado_namespace,
            dict_selectable_column="id",
            object_name="materia",
            xml_root_name="materias",
        )


def get_titulares_from_resultado(id):
    columns = ["tp.id", "tp.nombre", "CAST(tp.porcentaje AS DOUBLE) as porcentaje"]

    query = f"SELECT {', '.join(columns)} FROM titular_patente tp"
    query += " WHERE tp.patente_id = %s"

    params = []
    params.append(id)

    db = BaseDatos(database="prisma_resultado")
    result = db.ejecutarConsulta(query, params)

    return result


@resultado_namespace.route("/titulares/")
class TitularesPatente(Resource):
    @resultado_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "patente": {
                "name": "Patente",
                "description": "ID de la patente",
                "type": "int",
            },
        },
    )
    def get(self):
        """Inventores de una patente"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("patente", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=resultado_namespace)

        try:
            data = get_titulares_from_resultado(id)
        except:
            resultado_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested=nested,
            namespace=resultado_namespace,
            dict_selectable_column="id",
            object_name="titular",
            xml_root_name="titulares",
        )


def get_inventores_from_resultado(id):
    columns = ["ip.id", "ip.nombre", "ip.investigador_id"]

    query = f"SELECT {', '.join(columns)} FROM inventor_patente ip"
    query += " WHERE ip.patente_id = %s"

    params = []
    params.append(id)

    db = BaseDatos(database="prisma_resultado")
    result = db.ejecutarConsulta(query, params)

    return result


@resultado_namespace.route("/inventores/")
class InventoresPatente(Resource):
    @resultado_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "patente": {
                "name": "Patente",
                "description": "ID de la patente",
                "type": "int",
            },
        },
    )
    def get(self):
        """Titulares de una patente"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("patente", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=resultado_namespace)

        try:
            data = get_inventores_from_resultado(id)
        except:
            resultado_namespace.abort(500, "Error del servidor")

        # Devolver respuesta

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested=nested,
            namespace=resultado_namespace,
            dict_selectable_column="id",
            object_name="inventor",
            xml_root_name="inventores",
        )
