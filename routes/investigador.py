from flask import request, jsonify, Response
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import (comprobar_api_key)
import utils.format as format
import config.global_config as gconfig

investigador_namespace = Namespace(
    'investigador', description="Búsqueda y datos de investigadores")

global_responses = gconfig.responses

global_params = gconfig.params
global_params = {**global_params,
                 'inactivos': {
                     'name': 'ID',
                     'description': 'Incluir investigadores que han finalizado su relación con la US',
                     'type': 'bool',
                     'enum': ["True", "False"],
                 }, }


# COLUMNAS DEVUELTAS EN LAS CONSULTAS DE INVESTIGADOR

columns = ["i.idInvestigador as prisma", "concat(i.apellidos, ', ', i.nombre) as nombre_administrativo", "i.email as email",
           "orcid.valor as identificador_orcid", "dialnet.valor as identificador_dialnet", "idus.valor as identificador_idus", "researcherid.valor as identificador_researcherid",
           "scholar.valor as identificador_scholar", "scopus.valor as identificador_scopus", "sica.valor as identificador_sica", "sisius.valor as identificador_sisius", "wos.valor as identificador_wos",
           "categoria.nombre as categoria_nombre", "categoria.idCategoria as categoria_id",
           "area.nombre as area_conocimiento_nombre", "area.idArea as area_id",
           "departamento.nombre as departamento_nombre", "departamento.idDepartamento as departamento_id",
           "grupo.nombre as grupo_nombre", "grupo.idGrupo as grupo_id",
           "centro.nombre as centro_nombre", "centro.idCentro as centro_id",]

# LEFT JOINS

# Plantilla para left joins de identificadores de investigador
plantilla_ids = " LEFT JOIN (SELECT idInvestigador, valor FROM i_identificador_investigador WHERE tipo = '{0}') as {0} ON {0}.idInvestigador = i.idInvestigador"

left_joins = [plantilla_ids.format("orcid"),
              plantilla_ids.format("dialnet"),
              plantilla_ids.format("idus"),
              plantilla_ids.format("researcherid"),
              plantilla_ids.format("scholar"),
              plantilla_ids.format("scopus"),
              plantilla_ids.format("sica"),
              plantilla_ids.format("sisius"),
              plantilla_ids.format("wos"),

              " LEFT JOIN i_categoria categoria ON categoria.idCategoria = i.idCategoria",
              " LEFT JOIN i_area area ON area.idArea = i.idArea",
              " LEFT JOIN i_departamento departamento ON departamento.idDepartamento = i.idDepartamento",
              " LEFT JOIN i_grupo grupo ON grupo.idGrupo = i.idGrupo",
              " LEFT JOIN i_centro centro ON centro.idCentro = i.idCentro",]

# QUERY BASE

base_query = f"SELECT {', '.join(columns)} " + "FROM {} i"

# Prefijos para agrupar items en la consulta

nested = {"identificador": "identificadores",
          "categoria": "categoria",
          "area": "area",
          "departamento": "departamento",
          "grupo": "grupo",
          "centro": "centro", }


def merge_query(query: str, left_joins: str, inactivos: bool = False) -> str:

    tabla_investigador = "i_investigador" if inactivos else "i_investigador_activo"
    result = query.format(tabla_investigador)
    result += " ".join(left_joins)

    return result


@investigador_namespace.route('/')
class ResumenInvestigador(Resource):
    @investigador_namespace.doc(
        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={**global_params,
                'id': {
                    'name': 'ID',
                    'description': 'ID del investigador',
                    'type': 'int',
                }, }

    )
    def get(self):
        """Devuelve un resumen de datos de un investigador."""
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
        comprobar_api_key(api_key=api_key, namespace=investigador_namespace)

        params = []
        query = merge_query(base_query, left_joins, inactivos)

        query += " WHERE i.idInvestigador = %s"

        params.append(id)

        try:
            db = BaseDatos()
            data = db.ejecutarConsulta(query, params)
        except:
            investigador_namespace.abort(500, 'Error del servidor')

        # Comprobar el tipo de output esperado

        if 'json' in accept_type:
            dict_data = format.dict_from_table(
                data, "prisma", "investigador", nested)
            json_data = format.dict_to_json(dict_data)
            response = Response(json_data, mimetype='application/json')
            return response

        elif 'xml' in accept_type:
            dict_data = format.dict_from_table(
                data, "prisma", "investigador", nested)
            xml_data = format.dict_to_xml(
                dict_data, root_name=None, object_name="investigador")
            response = Response(xml_data, mimetype='application/xml')
            return response

        elif 'csv' in accept_type:
            csv_data = format.format_csv(data)
            return Response(csv_data, mimetype='text/csv')

        else:
            investigador_namespace.abort(406, 'Formato de salida no soportado')


@investigador_namespace.route('es/')
class BusquedaInvestigadores(Resource):
    @investigador_namespace.doc(

        responses=global_responses,

        produces=['application/json', 'application/xml', 'text/csv'],

        params={
            **global_params,
            'nombre': {
                'name': 'Nombre',
                'description': 'Nombre del investigador',
                'type': 'string',
            },
            'apellidos': {
                'name': 'Apellidos',
                'description': 'Apellidos del investigador',
                'type': 'string',
            },
            'email': {
                'name': 'Email',
                'description': 'Email del investigador',
                'type': 'string',
            },
            'departamento': {
                'name': 'Departamento',
                'description': 'ID del departamento',
                'type': 'string',
            },
            'grupo': {
                'name': 'Grupo',
                'description': 'ID del grupo de investigación',
                'type': 'string',
            },
            'area': {
                'name': 'Área',
                'description': 'ID del área de conocimiento',
                'type': 'string',
            },
            'instituto': {
                'name': 'Instituto',
                'description': 'ID del instituto',
                'type': 'string',
            },
            'centro': {
                'name': 'Centro',
                'description': 'ID del centro',
                'type': 'string',
            },
            'doctorado': {
                'name': 'Instituto',
                'description': 'ID del programa de doctorado',
                'type': 'string',
            },

        })
    def get(self):
        """Devuelve una lista de investigadores que cumpla simultáneamente todos los campos de búsqueda utilizados."""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        api_key = nombre = args.get('api_key', None)
        nombre = args.get('nombre', None)
        apellidos = args.get('apellidos', None)
        email = args.get('email', None)
        departamento = args.get('departamento', None)
        grupo = args.get('grupo', None)
        area = args.get('area', None)
        instituto = args.get('instituto', None)
        centro = args.get('centro', None)
        doctorado = args.get('doctorado', None)
        inactivos = True if (args.get('inactivos', "False").lower()
                             == "true") else False
        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=investigador_namespace)

        query = merge_query(base_query, left_joins, inactivos)

        conditions = []
        params = []

        # Comprobar qué parámetros de búsqueda están activos y generar filtros para la consulta
        if nombre:
            conditions.append(
                "nombre COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')")
            params.append(nombre)
        if apellidos:
            conditions.append(
                "apellidos COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')")
            params.append(apellidos)
        if email:
            conditions.append(
                "email COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')")
            params.append(email)
        if departamento:
            conditions.append("idDepartamento = %s")
            params.append(departamento)
        if grupo:
            conditions.append("idGrupo = %s")
            params.append(grupo)
        if area:
            conditions.append("idArea = %s")
            params.append(area)
        if instituto:
            conditions.append(
                "i_investigador.idInvestigador IN (SELECT idInvestigador FROM i_miembro_instituto WHERE idInstituto = %s)")
            params.append(instituto)
        if centro:
            conditions.append("idCentro = %s")
            params.append(centro)
        if doctorado:
            conditions.append(
                "i_investigador.idInvestigador IN (SELECT idInvestigador FROM i_profesor_doctorado WHERE idDoctorado = %s)")
            params.append(doctorado)

        # Decidir si se filtra o no por investigadores activos

        # Concatenar las queries de campos de búsqueda
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"

        try:
            db = BaseDatos()
            data = db.ejecutarConsulta(query, params)
        except:
            investigador_namespace.abort(500, 'Error del servidor')

        # Comprobar el tipo de output esperado

        if 'json' in accept_type:
            dict_data = format.dict_from_table(
                data, "prisma", "investigador", nested)
            json_data = format.dict_to_json(dict_data)
            response = Response(json_data, mimetype='application/json')
            return response

        elif 'xml' in accept_type:
            dict_data = format.dict_from_table(
                data, "prisma", "investigador", nested)
            xml_data = format.dict_to_xml(
                dict_data, root_name="investigadores", object_name="investigador")
            response = Response(xml_data, mimetype='application/xml')
            return response

        elif 'csv' in accept_type:
            csv_data = format.format_csv(data)
            return Response(csv_data, mimetype='text/csv')

        else:
            investigador_namespace.abort(406, 'Formato de salida no soportado')
