from flask_restx import Namespace, Resource
from flask import Response, make_response, request, jsonify, session
from db.conexion import BaseDatos
from models.investigador import Investigador
from routes.carga import consultas_cargas
from routes.carga.fuente.metricas.clarivate_journals import iniciar_carga
from routes.carga.investigador.centros_censo.carga import carga_centros_censados
from routes.carga.publicacion.idus.parser import IdusParser
from routes.carga.publicacion.scopus.parser import ScopusParser
from routes.carga.investigador.centros_censo.procesado import procesado_fichero
from routes.carga.investigador.erasmus_plus.procesado import (
    procesado_fichero_erasmus_plus,
)
from routes.carga.publicacion.carga_publicacion_por_investigador import (
    carga_publicaciones_investigador,
)
from routes.carga.publicacion.idus.carga import CargaPublicacionIdus
from routes.carga.publicacion.idus.parser import IdusParser
from routes.carga.publicacion.idus.xml_doi import xmlDoiIdus
from routes.carga.publicacion.scopus.carga import CargaPublicacionScopus
from routes.carga.publicacion.wos.carga import CargaPublicacionWos
from routes.carga.publicacion.openalex.carga import CargaPublicacionOpenalex
from routes.carga.publicacion.zenodo.carga import CargaPublicacionZenodo
from routes.carga.publicacion.crossref.carga import CargaPublicacionCrossref

from security.check_users import es_admin, es_editor
from celery import current_app
from routes.carga.investigador.grupos.actualizar_sica import (
    actualizar_tabla_sica,
    actualizar_grupos_sica,
)
from routes.carga.investigador.erasmus_plus.carga import (
    carga_erasmus_plus,
)
from datetime import datetime
from logger.async_request import AsyncRequest
import re
from utils.format import dataframe_to_json

carga_namespace = Namespace("carga", doc=False)


# ********************************
# **** CARGA DE GRUPOS DE INVESTIGACIÓN ****
# ********************************
@carga_namespace.route("/investigador/grupos", doc=False, endpoint="carga_grupos")
class CargaGrupos(Resource):
    def post(self):
        if not es_admin():
            return {"message": "No autorizado"}, 401
        if "files[]" not in request.files:
            return {"error": "No se han encontrado archivos en la petición"}, 400

        files = request.files.getlist("files[]")

        for file in files:
            file_path = "/app/temp/" + file.filename
            file.save(file_path)
            actualizar_tabla_sica(file_path)

        actualizar_grupos_sica()

        return None


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

            if not es_admin(api_key=api_key):
                return {"message": "No autorizado"}, 401
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

            # Verificar si el usuario es administrador (función de seguridad ya definida)
            if not es_admin(api_key=api_key):
                return {"message": "No autorizado"}, 401

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


# ********************************
# **** CARGA DE FUENTES DE DATOS ****
# ********************************
@carga_namespace.route(
    "/fuente/wos_journals/", doc=False, endpoint="carga_wos_journals"
)
class CargaWosJournals(Resource):
    def get(self):

        args = request.args
        api_key = args.get("api_key")

        current_year = datetime.now().year

        fuentes = args.get("fuentes", "todas")
        inicio = int(args.get("inicio", current_year - 1))
        fin = int(args.get("fin", current_year - 1))

        if not es_admin(api_key=api_key) or (
            es_editor(api_key=api_key) and fuentes != "todas"
        ):
            return {"message": "No autorizado"}, 401

        result = iniciar_carga(fuentes, inicio, fin)

        return {"message": result}, 200


# ********************************
# **** CARGA DE PUBLICACIONES IDUS ****
# ********************************
@carga_namespace.route(
    "/publicacion/idus/", doc=False, endpoint="carga_publicacion_idus"
)
class CargarPublicacionIdus(Resource):
    def get(self):
        args = request.args

        handle = args.get("handle", None)

        try:
            parser = IdusParser(handle=handle)
            json = parser.datos_carga_publicacion.to_json()

            return Response(json, content_type="application/json; charset=utf-8")

        except Exception:
            return {"message": "Error inesperado"}, 500


@carga_namespace.route(
    "/publicacion/idus/doi_xml/", doc=False, endpoint="carga_doi_xml"
)
class CargaDoiIdus(Resource):
    def get(self):
        args = request.args

        handle = args.get("handle", None)

        try:
            xml_doi = xmlDoiIdus(handle=handle)
            xml = xml_doi.xml
            response = make_response(xml)

            filename = xml_doi.handle.replace("/", "_")
            response.headers["Content-Disposition"] = (
                f"attachment; filename={filename}.xml"
            )
            response.headers["Content-Type"] = "application/xml"

            return response

        except Exception:
            return {"message": "Error inesperado"}, 500


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
        api_key = args.get("api_key")

        # if not es_admin():
        #    return {"message": "No autorizado"}, 401

        try:
            id_publicacion = None

            # TODO: Implementar recursividad para hacer una búsqueda general si se encuentra un DOI
            match tipo:
                case "scopus":
                    id_publicacion = (
                        CargaPublicacionScopus().carga_publicacion(tipo=tipo, id=id)
                        or id_publicacion
                    )
                case "pubmed" | "wos":
                    id_publicacion = (
                        CargaPublicacionWos().carga_publicacion(tipo=tipo, id=id)
                        or id_publicacion
                    )
                case "openalex":
                    id_publicacion = (
                        CargaPublicacionOpenalex().carga_publicacion(tipo=tipo, id=id)
                        or id_publicacion
                    )
                case "zenodo_id":
                    id_publicacion = (
                        CargaPublicacionZenodo().carga_publicacion(tipo=tipo, id=id)
                        or id_publicacion
                    )
                case "doi":
                    id_publicacion = (
                        CargaPublicacionScopus().carga_publicacion(tipo=tipo, id=id)
                        or id_publicacion
                    )
                    id_publicacion = (
                        CargaPublicacionWos().carga_publicacion(tipo=tipo, id=id)
                        or id_publicacion
                    )
                    id_publicacion = (
                        CargaPublicacionOpenalex().carga_publicacion(tipo=tipo, id=id)
                        or id_publicacion
                    )
                    id_publicacion = (
                        CargaPublicacionCrossref().carga_publicacion(tipo=tipo, id=id)
                        or id_publicacion
                    )

                case "idus":
                    id_publicacion = (
                        CargaPublicacionIdus().cargar_publicacion_por_handle(id)
                        or id_publicacion
                    )

            id_publicacion = id_publicacion or 0
            return {"id_publicacion": id_publicacion}, 200

        except Exception as e:
            return {"message": "Error inesperado"}, 500


# **** CARGA DE PUBLICACIONES MASIVO: TODAS LAS PUBLICACIONES POR INVESTIGADOR ****
@carga_namespace.route(
    "/publicacion/importar_publicaciones_por_investigador",
    doc=False,
    endpoint="importar_publicaciones_por_investigador",
)
class CargaPublicacionImportar(Resource):
    def get(self):

        args = request.args
        id = args.get("id", None).strip()
        api_key = args.get("api_key")

        if not es_admin(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            carga_publicaciones_investigador(
                id_investigador=id, agno_inicio=None, agno_fin=None
            )

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# **** CARGA DE PUBLICACIONES MASIVO: TODAS LAS PUBLICACIONES INVESTIGADORES ACTIVOS +- 1 AÑO ****
@carga_namespace.route(
    "/publicacion/importar_publicaciones_masivo",
    doc=False,
    endpoint="importar_publicaciones_masivo",
)
class ImportarPublicacionesMasivo(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_admin(api_key=api_key):
            return {"message": "No autorizado"}, 401

        agno_actual = current_year = datetime.now().year
        agno_inicio = agno_actual - 1
        agno_fin = agno_actual + 1
        try:
            investigadores = consultas_cargas.get_investigadores_activos()
            for key, value in investigadores.items():
                carga_publicaciones_investigador(
                    id_investigador=key, agno_inicio=agno_inicio, agno_fin=agno_fin
                )
                pass
        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500
