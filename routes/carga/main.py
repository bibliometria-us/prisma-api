from flask_restx import Namespace, Resource
from flask import Response, make_response, request, jsonify
from models.investigador import Investigador
from routes.carga import consultas_cargas
from routes.carga.fuente.metricas.clarivate_journals import iniciar_carga
from routes.carga.publicacion.carga_publicacion_por_investigador import (
    carga_publicaciones_investigador,
)
from routes.carga.publicacion.idus.carga import CargaPublicacionIdus
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
from datetime import datetime
import re


carga_namespace = Namespace("carga", doc=False)


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


@carga_namespace.route(
    "/publicacion/idus/", doc=False, endpoint="carga_publicacion_idus"
)
class CargaIdus(Resource):
    def get(self):
        args = request.args

        handle = args.get("handle", None)

        try:

            carga = CargaPublicacionIdus()
            carga.cargar_publicacion_por_handle(handle)

            # TODO: Gestionar excepciones y devolver un mensaje en función del resultado

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
    "/publicacion/importar/", doc=False, endpoint="carga_publicacion_importar"
)
class CargaPublicacionImportar(Resource):
    def get(self):

        args = request.args
        tipo = args.get("tipo", None).strip()
        id = args.get("id", None).strip()

        try:
            match tipo:
                case "scopus_id":
                    CargaPublicacionScopus().carga_publicacion(tipo=tipo, id=id)
                case "pubmed_id" | "wos_id":
                    CargaPublicacionWos().carga_publicacion(tipo=tipo, id=id)
                case "openalex_id":
                    CargaPublicacionOpenalex().carga_publicacion(tipo=tipo, id=id)
                case "zenodo_id":
                    CargaPublicacionZenodo().carga_publicacion(tipo=tipo, id=id)
                case "doi":
                    # TODO: REVISAR ORDEN EJECUCIÓN
                    CargaPublicacionScopus().carga_publicacion(tipo=tipo, id=id)
                    CargaPublicacionWos().carga_publicacion(tipo=tipo, id=id)
                    CargaPublicacionOpenalex().carga_publicacion(tipo=tipo, id=id)
                    CargaPublicacionZenodo().carga_publicacion(tipo=tipo, id=id)
                    CargaPublicacionCrossref().carga_publicacion(tipo=tipo, id=id)
                # TODO: QUEDA IDUS

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# **** CARGA DE PUBLICACIONES MASIVO: TODAS LAS PUBLICACIONES POR INVESTIGADOR ****
@carga_namespace.route(
    "/publicacion/importar_publicaciones_por_investigador/",
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
    "/publicacion/importar_publicaciones_masivo/",
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
