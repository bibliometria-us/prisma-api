from flask import Flask, request, jsonify, Response
from flask_restx import Api, Namespace, Resource
from db.conexion import BaseDatos
import utils.format as format

prisma_base_url = "https://bibliometria.us.es/prisma"

app = Flask(__name__)
api = Api(app, version="1.0", title="Prisma API")

investigador_namespace = Namespace(
    'investigador', description="Búsqueda y datos de investigadores")

api.add_namespace(investigador_namespace)

# ERRORES GLOBALES


@api.errorhandler(ValueError)  # Custom error handler for ValueError
def handle_invalid_accept_header(error):
    """Error handler for Invalid Accept header."""
    return {'message': 'Formato de salida no soportado'}, 406


# CLASE BASE, CONTIENE TODOS LOS DECORADORES COMUNES DE DOCUMENTACIÓN

@api.doc(responses={200: 'La solicitud ha tenido éxito',
                    406: 'Formato de salida no soportado',
                    500: 'Error del servidor',
                    })
class BaseResource(Resource):
    pass


# ---------------
# INVESTIGADORES
# ---------------

@investigador_namespace.route('/busqueda')
class BusquedaInvestigadores(BaseResource):
    @api.doc(params={
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
            'name': 'Instituto',
            'description': 'ID del instituto',
            'type': 'centro',
        },
        'doctorado': {
            'name': 'Instituto',
            'description': 'ID del programa de doctorado',
            'type': 'centro',
        },

    })
    @api.doc(produces=['application/json', 'text/csv'])
    def get(self):
        """Devuelve la lista de investigadores."""
        headers = request.headers
        args = request.args
        accept_type = headers.get('Accept', 'application/json')

        # Cargar argumentos de búsqueda
        nombre = args.get('nombre', None)
        apellidos = args.get('apellidos', None)
        email = args.get('email', None)
        departamento = args.get('departamento', None)
        grupo = args.get('grupo', None)
        area = args.get('area', None)
        instituto = args.get('instituto', None)
        centro = args.get('centro', None)
        doctorado = args.get('doctorado', None)

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
            api.abort(500, 'Error del servidor')

        # Comprobar el tipo de output esperado
        if accept_type == 'application/json':
            dict_data = format.dict_from_table(data, "idInvestigador")
            return jsonify(dict_data)

        elif accept_type == 'text/csv':
            csv_data = format.format_csv(data)
            return Response(csv_data, mimetype='text/csv')

        else:
            api.abort(406, 'Formato de salida no soportado')


if __name__ == '__main__':
    app.run()
