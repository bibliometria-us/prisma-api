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


@investigador_namespace.route('/')
class ResumenInvestigador(Resource):
    @investigador_namespace.doc(
        responses=global_responses,

        produces=['application/json'],

        params={**global_params,
                'id': {
                    'name': 'ID',
                    'description': 'ID del investigador',
                    'type': 'int',
                },
                'todos': {
                    'name': 'todos',
                    'description': 'Devolver una lista de todos los investigadores (desactivado por defecto)',
                    'type': 'bool',
                    'enum': ["True", "False"]
                }}

    )
    def get(self):
        """Devuelve un resumen de datos de un investigador."""
        headers = request.headers
        args = request.args
        # accept_type = headers.get('Accept', 'application/json')
        accept_type = 'application/json'
        # Cargar argumentos de búsqueda
        api_key = args.get('api_key', None)
        id = args.get('id', None)
        todos = True if (args.get('todos', "False").lower()
                         == "true") else False

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=investigador_namespace)

        params = []
        # Columnas a mostrar
        columns = ["i.idInvestigador as prisma", "concat(i.apellidos, ', ', i.nombre) as nombre_administrativo",
                   "orcid.valor as orcid", "dialnet.valor as dialnet", "idus.valor as idus", "researcherid.valor as researcherid",
                   "scholar.valor as scholar", "scopus.valor as scopus", "sica.valor as sica", "sisius.valor as sisius", "wos.valor as wos"]

        # Left joins para obtener identificadores y datos de otras tablas

        plantilla_ids = " LEFT JOIN (SELECT idInvestigador, valor FROM i_identificador_investigador WHERE tipo = '{0}') as {0} ON {0}.idInvestigador = i.idInvestigador"

        left_joins = [plantilla_ids.format("orcid"),
                      plantilla_ids.format("dialnet"),
                      plantilla_ids.format("idus"),
                      plantilla_ids.format("researcherid"),
                      plantilla_ids.format("scholar"),
                      plantilla_ids.format("scopus"),
                      plantilla_ids.format("sica"),
                      plantilla_ids.format("sisius"),
                      plantilla_ids.format("wos")]

        query = f"SELECT {', '.join(columns)} FROM i_investigador_activo i"

        query += f"{' '.join(left_joins)}"

        if not todos:
            query += " WHERE i.idInvestigador = %s"
            params.append(id)

        try:
            db = BaseDatos()
            data = db.ejecutarConsulta(query, params)
        except:
            investigador_namespace.abort(500, 'Error del servidor')

        # Comprobar el tipo de output esperado
        if accept_type == 'application/json':
            dict_data = format.dict_from_table(data, "prisma")
            return jsonify(dict_data)

        elif accept_type == 'text/csv':
            csv_data = format.format_csv(data)
            return Response(csv_data, mimetype='text/csv')

        else:
            investigador_namespace.abort(406, 'Formato de salida no soportado')


@investigador_namespace.route('/busqueda')
class BusquedaInvestigadores(Resource):
    @investigador_namespace.doc(

        responses=global_responses,

        produces=['application/json', 'text/csv'],

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
            'activo': {
                'name': 'Activo',
                'description': 'Indica si se buscarán solo investigadores activos (1, por defecto) o incluir investigadores inactivos (0)',
                'type': 'bool',
                'enum': ["True", "False"]
            },

        })
    def get(self):
        """Devuelve una lista de investigadores que cumpla simultáneamente todos los campos de búsqueda utilizados."""
        headers = request.headers
        args = request.args
        accept_type = headers.get('Accept', 'application/json')

        # Cargar argumentos de búsqueda
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
        activo = True if (args.get('activo', "True").lower()
                          == "true") else False

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=investigador_namespace)

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

        # Lista de columnas devueltas
        columns = ["idInvestigador", "nombre", "apellidos", "email", "idCategoria", "idArea", "fechaContratacion",
                   "idDepartamento", "idGrupo", "idCentro", "nacionalidad", "sexo", "fechaNombramiento", "fechaActualizacion"]

        # Construir la consulta SQL parametrizada
        query = f"SELECT {', '.join(columns)} FROM i_investigador"

        # Decidir si se filtra o no por investigadores activos
        if activo:
            query += " JOIN (SELECT idInvestigador as id FROM i) a ON i_investigador.idInvestigador = a.id"

        # Concatenar las queries de campos de búsqueda
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"

        try:
            db = BaseDatos()
            data = db.ejecutarConsulta(query, params)
        except:
            investigador_namespace.abort(500, 'Error del servidor')

        # Comprobar el tipo de output esperado
        if accept_type == 'application/json':
            dict_data = format.dict_from_table(data, "idInvestigador")
            return jsonify(dict_data)

        elif accept_type == 'text/csv':
            csv_data = format.format_csv(data)
            return Response(csv_data, mimetype='text/csv')

        else:
            investigador_namespace.abort(406, 'Formato de salida no soportado')
