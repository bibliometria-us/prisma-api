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
                'type': 'centro',
            },
            'doctorado': {
                'name': 'Instituto',
                'description': 'ID del programa de doctorado',
                'type': 'centro',
            },

        })
    def get(self):
        """Devuelve la lista de investigadores."""
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

        # Comprobar api_key
        if not (request.referrer and request.referrer.startswith("http://127.0.0.1:8000")):
            if not api_key or not comprobar_api_key(api_key):
                investigador_namespace.abort(401, 'API Key inválida')

        conditions = []
        params = []

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
        query = "SELECT {} FROM i_investigador".format(", ".join(columns))

        if conditions:
            query += " WHERE {}".format(" AND ".join(conditions))

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
