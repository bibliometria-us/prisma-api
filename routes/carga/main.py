import os
import tempfile
import threading
from flask_restx import Namespace, Resource
from flask import Response, make_response, request, jsonify, session
import pandas as pd
from db.conexion import BaseDatos
from models.investigador import Investigador
from routes.carga import consultas_cargas
from routes.carga.financiacion.carga_proyectos import carga_proyectos
from routes.carga.fuente.metricas.acuerdos_transformativos.acuerdos_transformativos import (
    carga_acuerdos_transformativos,
    transformar_fichero,
)
from routes.carga.fuente.metricas.acuerdos_transformativos.exception import (
    ErrorAT,
)
from routes.carga.fuente.metricas.clarivate_journals import iniciar_carga
from routes.carga.fuente.metricas.sjr.carga import CargaSJR
from routes.carga.fuente.metricas.sjr.importar import ImportarSJR
from routes.carga.investigador.centros_censo.carga import carga_centros_censados
from routes.carga.investigador.investigador.RRHH.carga import ImportarInvestigadoresRRHH
from routes.carga.investigador.grupos.carga_sica import carga_sica
from routes.carga.publicacion.carga_publicacion_por_investigador import (
    CargaPublicacionesBloque,
)
from routes.carga.publicacion.idus.parser import IdusParser
from routes.carga.publicacion.importacion_publicacion import ImportacionPublicacion
from routes.carga.publicacion.scopus.parser import ScopusParser
from routes.carga.investigador.centros_censo.procesado import procesado_fichero
from routes.carga.investigador.erasmus_plus.procesado import (
    procesado_fichero_erasmus_plus,
)

from routes.usuario import get_user_data
from saml.session_utils import get_email_from_session
from security.check_users import get_email_from_api_key
from celery import current_app
from routes.carga.investigador.erasmus_plus.carga import (
    carga_erasmus_plus,
)
from datetime import datetime
from logger.async_request import AsyncRequest
import re
from utils.format import dataframe_to_json, flask_csv_to_df

carga_namespace = Namespace("carga", doc=False)


# ********************************
# **** CARGA DE GRUPOS DE INVESTIGACIÓN ****
# ********************************
@carga_namespace.route("/investigador/grupos", doc=False, endpoint="carga_grupos")
class CargaGrupos(Resource):
    def post(self):
        # Extraer api_key como en los otros endpoints
        args = request.args
        api_key = args.get("api_key")

        if "files[]" not in request.files:
            return {"error": "No se han encontrado archivos en la petición"}, 400

        files = request.files.getlist("files[]")

        uploaded_filenames = {f.filename for f in files}
        expected_filenames = {
            "T_GRUPOS.csv",
            "T_INSTITUCIONES.csv",
            "T_INVESTIGADORES.csv",
            "T_INVESTIGADORES_GRUPO.csv",
        }

        missing_files = expected_filenames - uploaded_filenames

        if missing_files:
            return {
                "message": f"Faltan los siguientes ficheros: {str(missing_files)}"
            }, 400

        try:

            file_dict = {
                f.filename.lower().removesuffix(".csv"): pd.read_csv(
                    f.stream,
                    sep=";",
                    quotechar='"',
                    encoding="utf-8-sig",  # 'utf-8-sig' handles the BOM if it exists
                )
                for f in files
            }
        except Exception as e:
            {"message": "Error procesando los ficheros introducidos"}, 502

        try:
            thread = threading.Thread(target=carga_sica, kwargs={"files": file_dict})
            thread.start()
        except Exception as e:
            {"message": "Error inesperado"}, 502

        return {"message": "Carga iniciada satisfactoriamente"}, 200


# ********************************
# **** CARGA DE CENTROS DE CENSO ****
# ********************************
@carga_namespace.route(
    "/investigador/centros_censo/", doc=False, endpoint="carga_centros_censo"
)
class CargaCentrosCenso(Resource):
    def post(self):
        # TODO: Implementar llamada a la API para la carga del fichero de centros de censo.
        args = request.args
        try:
            api_key = args.get("api_key")

            if "files[]" not in request.files:
                return {"error": "No se han encontrado archivos en la petición"}, 400

            file = request.files.getlist("files[]")[0]

            if not file.filename.endswith((".xls", ".xlsx")):
                return {"error": "El archivo no es un Excel válido"}, 400

            # 1. Lectura de fichero. Crear estos métodos en routes/carga/investigador/centros_censo/procesado.py
            csv_path = procesado_fichero(file)
            # 2. Crear un AsyncRequest al que le pasas la ruta del fichero como parámetro
            params = {"ruta": csv_path}
            async_request = AsyncRequest(
                email="bibliometria@us.es",
                params=params,
                request_type="carga_centros_censo",
            )
            # 3. Llamar a la función de Celery etiquetada como "carga_centros_censo" con la ID del AsyncRequest creado como parámetro
            current_app.tasks["carga_centros_censo"].apply_async([async_request.id])
        except Exception as e:
            # Capturar la excepción e imprimir el mensaje de error
            print(f"Ocurrió un error: {e}")


# ********************************
# **** CARGA DE INVESTIGADORES ERASMUS+ ****
# ********************************
@carga_namespace.route(
    "/investigador/erasmus_plus/", doc=False, endpoint="carga_erasmus_plus"
)
class CargaErasmusPlus(Resource):
    def post(self):
        # Obtener parámetros de la URL (por ejemplo, clave de API para autorización)
        args = request.args
        try:
            api_key = args.get("api_key")

            # Verificar que se haya enviado un archivo con la clave esperada
            if "files[]" not in request.files:
                return {"error": "No se ha enviado ningún archivo Excel"}, 400

            # Obtener el primer archivo (solo admitimos uno en esta carga)
            file = request.files.getlist("files[]")[0]

            # Validar la extensión del archivo (debe ser Excel)
            if not file.filename.endswith((".xls", ".xlsx")):
                return {"error": "El archivo no es un Excel válido"}, 400

            # 1. Procesar el fichero y guardarlo temporalmente
            excel_path = procesado_fichero_erasmus_plus(
                file
            )  # Puedes renombrar este método si no es CSV

            # ----> EJECUCIÓN DIRECTA, SIN CELERY (SINCRÓNA) - DEBUG
            # from routes.carga.investigador.erasmus_plus.carga import carga_erasmus_plus

            # carga_erasmus_plus(excel_path)
            # # Devolver respuesta directa
            # return {"message": "Carga Erasmus+ completada correctamente"}, 200

            # ----> EJECUCIÓN CON CELERY (ASINCRÓNA)
            # 2. Preparar los parámetros que usará Celery (por ahora solo la ruta del fichero)
            params = {"ruta": excel_path}

            # 3. Crear una solicitud asíncrona para ejecutar la tarea en segundo plano
            async_request = AsyncRequest(
                # email="bibliometria@us.es",  # Email que recibirá la notificación
                email="fmacias3@us.es",  # Email que recibirá la notificación
                params=params,  # Parámetros que necesita la tarea
                request_type="carga_erasmus_plus",  # Nombre que enlaza con la tarea de Celery
            )

            # 4. Lanzar la tarea asíncrona con Celery, pasando la ID del async_request
            current_app.tasks["carga_erasmus_plus"].apply_async([async_request.id])

            # Respuesta inmediata al cliente (Postman o frontend)
            return {"message": "Carga lanzada correctamente"}, 202

        except Exception as e:
            print(f"Error en la carga Erasmus+: {e}")
            return {"error": "Error inesperado en el servidor"}, 500


@carga_namespace.route(
    "/investigador/rrhh", doc=False, endpoint="carga_investigador_rrhh"
)
class CargaInvestigadorRRHHEndpoint(Resource):
    def post(self):
        # Obtener parámetros de la URL (por ejemplo, clave de API para autorización)
        args = request.args
        try:
            api_key = args.get("api_key")
            dry_run = args.get("dry_run", "false").lower() == "true"

            # Obtener los archivos enviados y guardarlos temporalmente
            files = request.files
            file_paths = {}

            for key, value in files.items():
                value.name = value.filename
                temp_file = tempfile.NamedTemporaryFile(delete=False)
                value.save(temp_file.name)
                file_paths[key] = temp_file.name

            # Ejecutar la carga de investigadores desde RRHH
            carga_investigador_rrhh = ImportarInvestigadoresRRHH()
            carga_investigador_rrhh.importar_investigadores_RRHH(
                file_paths=file_paths, dry_run=dry_run
            )

            return {
                "message": "Carga de investigadores desde RRHH completada correctamente"
            }, 200

        except Exception as e:
            print(f"Error en la carga de investigadores desde RRHH: {e}")
            return {"error": "Error inesperado en el servidor"}, 500


# ********************************
# **** CARGA DE FUENTES DE DATOS ****
# ********************************
@carga_namespace.route(
    "/fuente/wos_journals/", doc=False, endpoint="carga_wos_journals"
)
class CargaWosJournals(Resource):
    def get(self):

        args = request.args

        current_year = datetime.now().year

        fuentes = args.get("fuentes", "todas")
        inicio = int(args.get("inicio", current_year - 1))
        fin = int(args.get("fin", current_year - 1))

        result = iniciar_carga(fuentes, inicio, fin)

        return {"message": result}, 200


# ********************************
# **** CARGA DE PUBLICACIONES ****
# ********************************


# **** CARGA DE PUBLICACIONES INDIVIDUAL ****
@carga_namespace.route(
    "/publicacion/importar", doc=False, endpoint="carga_publicacion_importar"
)
class CargaPublicacionImportar(Resource):
    def get(self):

        args = request.args
        tipo = args.get("tipo", "").strip()
        id = args.get("id", "").strip()

        try:
            saml_user_data = get_user_data()
            usuario = saml_user_data.get("mail", [""])[0].split("@")[0] or None
        except Exception:
            usuario = None

        importacion = ImportacionPublicacion(id=id, tipo_id=tipo, autor=usuario)

        importacion.importar()

        id_publicacion = importacion.id_publicacion

        if id_publicacion == 0:
            if importacion.errores:
                return {
                    "error": ";\n ".join(importacion.errores),
                }, 400
            return {
                "error": "No se ha podido encontrar una publicación con el identificador aportado."
            }, 400

        return {"id_publicacion": id_publicacion}, 200


# **** CARGA DE PUBLICACIONES MASIVO: TODAS LAS PUBLICACIONES POR INVESTIGADOR ****
@carga_namespace.route(
    "/publicacion/carga_publicaciones_bloque",
    doc=False,
    endpoint="carga_publicaciones_bloque",
)
class CargaPublicacionImportar(Resource):
    def get(self):

        args = request.args
        id_investigador = args.get("id_investigador", "").strip()
        api_key = request.headers.get("X-API-Key")
        dry_run = args.get("dry_run", "false").lower() == "true"

        email = get_email_from_api_key(api_key) or get_email_from_session()

        try:
            carga = CargaPublicacionesBloque()
            thread = threading.Thread(
                target=carga.carga_publicaciones_investigador,
                kwargs={
                    "id_investigador": id_investigador,
                    "email": email,
                    "dry_run": dry_run,
                },
            )

            thread.start()
            return {"message": "Carga iniciada satisfactoriamente"}, 200
        except Exception as e:
            return {"error": "Error inesperado"}, 500


@carga_namespace.route(
    "/financiacion/carga_proyectos",
    doc=False,
    endpoint="carga_proyectos",
)
class CargaProyectos(Resource):
    def post(self):
        args = request.args
        api_key = args.get("api_key")

        request_files = request.files

        for file in request_files.values():
            file.name = file.filename

        contracts = flask_csv_to_df(request_files["contracts"])
        components = flask_csv_to_df(request_files["components"])
        projects = flask_csv_to_df(request_files["projects"])
        # external_projects = flask_csv_to_df(request_files["external_projects"])

        files = {
            "contracts": contracts,
            "components": components,
            "projects": projects,
            # "external_projects": external_projects,
        }

        try:
            thread = threading.Thread(target=carga_proyectos, kwargs={"files": files})
            thread.start()
        except KeyError as e:
            return {"message": str(e)}, 400

        return {"message": "Carga iniciada satisfactoriamente"}, 200


@carga_namespace.route(
    "/metricas/carga_at",
    doc=False,
    endpoint="carga_at",
)
class CargaAT(Resource):
    def post(self):
        args = request.args
        api_key = args.get("api_key")

        try:
            files = request.files.getlist("files[]")

            if len(files) == 0:
                return {"error": "No se han adjuntado ficheros adecuadamente"}, 400

            file = files[0]

            if not file.filename.endswith((".xlsx", ".xls")):
                return {"error": "Formato de fichero erróneo"}, 400

            data = transformar_fichero(file)
            thread = threading.Thread(
                target=carga_acuerdos_transformativos, kwargs={"data": data}
            )
            thread.start()

            return {"message": "Carga iniciada satisfactoriamente"}

        except ErrorAT as e:
            return {"error": str(e)}, 400
        except Exception as e:
            return {"error": "Error inesperado"}, 502


@carga_namespace.route(
    "/metricas/carga_sjr",
    doc=False,
    endpoint="carga_sjr",
)
class CargaMetricasSJR(Resource):
    def post(self):
        args = request.args
        api_key = args.get("api_key")
        year = args.get("year", int(args.get("year", datetime.now().year - 1)))
        dry_run = args.get("dry_run", "false").lower() == "true"

        files = request.files.getlist("files[]")

        if len(files) == 0:
            return {"error": "No se han adjuntado ficheros adecuadamente"}, 400

        file = files[0]

        if not file.filename.endswith((".csv")):
            return {"error": "Formato de fichero erróneo"}, 400

        try:
            carga_sjr = ImportarSJR(year=year)
            data = pd.read_csv(file, sep=";")

            thread = threading.Thread(
                target=carga_sjr.importar, kwargs={"data": data, "dry_run": dry_run}
            )
            thread.start()
            return {"message": "Carga iniciada satisfactoriamente"}, 200
        except Exception as e:
            return {"error": "Error inesperado"}, 502
