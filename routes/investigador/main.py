from flask import request, jsonify, Response
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from routes.investigador.colectivos.carga import cargar_colectivos_investigadores
from security.api_key import comprobar_api_key
from security.check_users import es_admin
from utils.timing import func_timer as timer
import utils.format as format
import utils.pages as pages
import utils.response as response
import config.global_config as gconfig

investigador_namespace = Namespace("investigador", description="Investigadores")

global_responses = gconfig.responses

global_params = gconfig.params
global_params_inactivos = {
    **global_params,
    "inactivos": {
        "name": "ID",
        "description": "Incluir investigadores que han finalizado su relación con la US",
        "type": "bool",
        "enum": ["True", "False"],
    },
}
paginate_params = gconfig.paginate_params

# COLUMNAS DEVUELTAS EN LAS CONSULTAS DE INVESTIGADOR

columns = [
    "i.idInvestigador as prisma",
    "concat(i.apellidos, ', ', i.nombre) as nombre_administrativo",
    "i.email as email",
    "DATE_FORMAT(i.fechaNombramiento, '%d/%m/%Y') as fecha_nombramiento",
    "orcid.valor as identificador_orcid",
    "dialnet.valor as identificador_dialnet",
    "idus.valor as identificador_idus",
    "researcherid.valor as identificador_researcherid",
    "scholar.valor as identificador_scholar",
    "scopus.valor as identificador_scopus",
    "sica.valor as identificador_sica",
    "sisius.valor as identificador_sisius",
    "wos.valor as identificador_wos",
    "CASE WHEN i.sexo = 0 THEN categoria.femenino ELSE categoria.nombre END AS categoria_nombre",
    "categoria.idCategoria as categoria_id",
    "area.nombre as area_conocimiento_nombre",
    "area.idArea as area_id",
    "departamento.nombre as departamento_nombre",
    "departamento.idDepartamento as departamento_id",
    "grupo.nombre as grupo_nombre",
    "grupo.idGrupo as grupo_id",
    "grupo.acronimo as grupo_acronimo",
    "grupo.rama as grupo_rama",
    "centro.nombre as centro_nombre",
    "centro.idCentro as centro_id",
]

count_prefix = ["COUNT(*) as cantidad"]
# LEFT JOINS

# Plantilla para left joins de identificadores de investigador
plantilla_ids = " LEFT JOIN (SELECT idInvestigador, valor FROM i_identificador_investigador WHERE tipo = '{0}') as {0} ON {0}.idInvestigador = i.idInvestigador"

left_joins = [
    plantilla_ids.format("orcid"),
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
    " LEFT JOIN i_centro centro ON centro.idCentro = i.idCentro",
    " LEFT JOIN i_miembro_instituto ON i.idInvestigador = i_miembro_instituto.idInvestigador",
]
# QUERY BASE

base_query = f"SELECT {', '.join(columns)} " + "FROM {} i"

# Prefijos para agrupar items en la consulta

nested = {
    "identificador": "identificadores",
    "categoria": "categoria",
    "area": "area",
    "departamento": "departamento",
    "grupo": "grupo",
    "centro": "centro",
}

# Fusiona las queries en una query utilizable


def merge_query(columns: str, left_joins: str, inactivos: bool = False) -> str:

    query = f"SELECT {', '.join(columns)} " + "FROM {} i"
    tabla_investigador = "i_investigador" if inactivos else "i_investigador_activo"
    result = query.format(tabla_investigador)
    result += " ".join(left_joins)
    return result


def get_investigador_from_id(columns, left_joins, inactivos, id):

    query = merge_query(columns, left_joins, inactivos)

    params = []
    query += " WHERE i.idInvestigador = %s"
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@investigador_namespace.route("/")
class Investigador(Resource):
    @investigador_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params_inactivos,
            "id": {
                "name": "ID",
                "description": "ID del investigador",
                "type": "int",
            },
        },
    )
    def get(self):
        """Información de un investigador"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("id", None)
        inactivos = (
            True if (args.get("inactivos", "False").lower() == "true") else False
        )

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=investigador_namespace)

        try:
            data = get_investigador_from_id(columns, left_joins, inactivos, id)
        except:
            investigador_namespace.abort(500, "Error del servidor")

        # Comprobar el tipo de output esperado

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested=nested,
            namespace=investigador_namespace,
            dict_selectable_column="prisma",
            object_name="investigador",
            xml_root_name="",
        )


def get_investigadores(
    columns, left_joins, inactivos, conditions, params, limit=None, offset=None
):

    query = merge_query(columns, left_joins, inactivos)

    query += conditions

    if limit is not None and offset is not None:
        query += " ORDER BY prisma"
        query += " LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@investigador_namespace.route("es/")
class Investigadores(Resource):
    @investigador_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params_inactivos,
            **paginate_params,
            "nombre": {
                "name": "Nombre",
                "description": "Nombre del investigador",
                "type": "string",
            },
            "apellidos": {
                "name": "Apellidos",
                "description": "Apellidos del investigador",
                "type": "string",
            },
            "email": {
                "name": "Email",
                "description": "Email del investigador",
                "type": "string",
            },
            "departamento": {
                "name": "Departamento",
                "description": "ID del departamento",
                "type": "string",
            },
            "grupo": {
                "name": "Grupo",
                "description": "ID del grupo de investigación",
                "type": "string",
            },
            "area": {
                "name": "Área",
                "description": "ID del área de conocimiento",
                "type": "string",
            },
            "instituto": {
                "name": "Instituto",
                "description": "ID del instituto",
                "type": "string",
            },
            "centro": {
                "name": "Centro",
                "description": "ID del centro",
                "type": "string",
            },
            "doctorado": {
                "name": "Doctorado",
                "description": "ID del programa de doctorado",
                "type": "string",
            },
        },
    )
    def get(self):
        """Búsqueda de investigadores"""

        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        pagina = int(args.get("pagina", 1))
        longitud_pagina = int(args.get("longitud_pagina", 100))
        nombre = args.get("nombre", None)
        apellidos = args.get("apellidos", None)
        email = args.get("email", None)
        departamento = args.get("departamento", None)
        grupo = args.get("grupo", None)
        area = args.get("area", None)
        instituto = args.get("instituto", None)
        centro = args.get("centro", None)
        doctorado = args.get("doctorado", None)
        inactivos = (
            True if (args.get("inactivos", "False").lower() == "true") else False
        )
        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=investigador_namespace)

        conditions = []
        params = []

        # Comprobar qué parámetros de búsqueda están activos y generar filtros para la consulta
        if nombre:
            conditions.append(
                "i.nombre COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(nombre)
        if apellidos:
            conditions.append(
                "i.apellidos COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(apellidos)
        if email:
            conditions.append(
                "i.email COLLATE utf8mb4_general_ci LIKE CONCAT('%', %s, '%')"
            )
            params.append(email)
        if departamento:
            conditions.append("i.idDepartamento = %s")
            params.append(departamento)
        if grupo:
            conditions.append("i.idGrupo = %s")
            params.append(grupo)
        if area:
            conditions.append("i.idArea = %s")
            params.append(area)
        if instituto:
            conditions.append(
                "i.idInvestigador IN (SELECT idInvestigador FROM i_miembro_instituto WHERE idInstituto = %s)"
            )
            params.append(instituto)
        if centro:
            conditions.append("i.idCentro = %s")
            params.append(centro)
        if doctorado:
            conditions.append(
                "i.idInvestigador IN (SELECT idInvestigador FROM i_profesor_doctorado WHERE idDoctorado = %s)"
            )
            params.append(doctorado)

        # Concatenar las queries de campos de búsqueda
        conditions_str = ""

        if conditions:
            conditions_str = f" WHERE {' AND '.join(conditions)}"

        amount = int(
            get_investigadores(
                count_prefix, left_joins, inactivos, conditions_str, params
            )[1][0]
        )

        longitud_pagina, offset = pages.get_page_offset(pagina, longitud_pagina, amount)

        try:
            data = get_investigadores(
                columns,
                left_joins,
                inactivos,
                conditions_str,
                params,
                longitud_pagina,
                offset,
            )
        except:
            investigador_namespace.abort(500, "Error del servidor")

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested=nested,
            namespace=investigador_namespace,
            dict_selectable_column="prisma",
            object_name="investigador",
            xml_root_name="investigadores",
        )


# INSTITUTOS DE UN INVESTIGADOR
query_institutos = "SELECT {} FROM i_miembro_instituto m_i"

institutos_columns = ["i.idInstituto as id", "i.nombre", "i.acronimo"]

institutos_left_joins = [" LEFT JOIN i_instituto i ON i.idInstituto = m_i.idInstituto"]


def get_institutos_from_investigador(id):
    params = []
    query = query_institutos.format(",".join(institutos_columns)) + " ".join(
        institutos_left_joins
    )
    query += " WHERE m_i.idInvestigador = %s"
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@investigador_namespace.route("/institutos/")
class InstitutosInvestigador(Resource):
    @investigador_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "id": {
                "name": "ID",
                "description": "ID del investigador",
                "type": "int",
            },
        },
    )
    def get(self):
        """Institutos de un investigador"""

        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("id", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=investigador_namespace)

        try:
            data = get_institutos_from_investigador(id)
        except:
            investigador_namespace.abort(500, "Error del servidor")

        # Comprobar el tipo de output esperado

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=investigador_namespace,
            dict_selectable_column="id",
            object_name="instituto",
            xml_root_name="institutos",
        )


# PUBLICACIONES DE UN INVESTIGADOR


@investigador_namespace.route("/publicaciones/")
class PublicacionesInvestigador(Resource):
    @investigador_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "id": {
                "name": "ID",
                "description": "ID del investigador",
                "type": "int",
            },
        },
    )
    def get(self):
        """Publicaciones de un investigador"""
        headers = request.headers
        args = request.args

        accept_type = args.get(
            "salida", headers.get("Accept", "application/json")
        ).replace("/", "")
        api_key = args.get("api_key", None)
        id = args.get("id", None)

        request_url = f"http://{request.host}/"
        request_urn = (
            f"publicaciones/?salida={accept_type}&api_key={api_key}&investigador={id}"
        )
        referrer = request.referrer
        return response.generate_response_from_uri(request_url, request_urn, referrer)


# PROGRAMAS DE DOCTORADO DE UN INVESTIGADOR
query_programas_doctorado = "SELECT {} FROM i_profesor_doctorado pd"

programas_doctorado_columns = ["d.idDoctorado as id", "d.nombre"]

programas_doctorado_left_joins = [
    " LEFT JOIN i_doctorado d ON d.idDoctorado = pd.idDoctorado"
]


def get_programas_doctorado_from_investigador(id):
    params = []
    query = query_programas_doctorado.format(
        ",".join(programas_doctorado_columns)
    ) + " ".join(programas_doctorado_left_joins)
    query += " WHERE pd.idInvestigador = %s"
    params.append(id)

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result


@investigador_namespace.route("/programas_doctorado/")
class ProgramasDoctoradoInvestigador(Resource):
    @investigador_namespace.doc(
        responses=global_responses,
        produces=["application/json", "application/xml", "text/csv"],
        params={
            **global_params,
            "id": {
                "name": "ID",
                "description": "ID del investigador",
                "type": "int",
            },
        },
    )
    def get(self):
        """Programas de doctorado de un investigador"""
        headers = request.headers
        args = request.args

        # Cargar argumentos de búsqueda
        accept_type = args.get("salida", headers.get("Accept", "application/json"))
        api_key = args.get("api_key", None)
        id = args.get("id", None)

        # Comprobar api_key
        comprobar_api_key(api_key=api_key, namespace=investigador_namespace)

        try:
            data = get_programas_doctorado_from_investigador(id)
        except:
            investigador_namespace.abort(500, "Error del servidor")

        # Comprobar el tipo de output esperado

        return response.generate_response(
            data=data,
            output_types=["json", "xml", "csv"],
            accept_type=accept_type,
            nested={},
            namespace=investigador_namespace,
            dict_selectable_column="id",
            object_name="programa_doctorado",
            xml_root_name=None,
        )


@investigador_namespace.route("/colectivos/carga")
class ColectivosInvestigador(Resource):
    @investigador_namespace.doc(doc=False)
    def post(self):
        if not es_admin():
            return {"message": "No autorizado"}, 401
        if "files[]" not in request.files:
            return {"error": "No se han encontrado archivos en la petición"}, 400

        files = request.files.getlist("files[]")

        file = files[0]

        data = format.flask_csv_to_matix(file)

        try:
            cargar_colectivos_investigadores(data)
        except Exception as e:
            return {"message": "error"}, 500

        return {"message": "Carga finalizada correctamente"}, 200
