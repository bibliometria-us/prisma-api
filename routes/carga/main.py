from flask_restx import Namespace, Resource
from flask import Response, make_response, request, jsonify
from models.investigador import Investigador
from routes.carga import consultas_cargas
from routes.carga.fuente.metricas.clarivate_journals import iniciar_carga
from routes.carga.investigador.centros_censo.carga import carga_centros_censados
from routes.carga.publicacion.idus.parser import IdusParser
from routes.carga.publicacion.scopus.parser import ScopusParser
from routes.carga.investigador.centros_censo.procesado import procesado_fichero
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
from datetime import datetime
from logger.async_request import AsyncRequest
import re
from utils.format import dataframe_to_json


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
class CargaPublicacionIdus(Resource):
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


# *************************************
# **** QUALITY RULES PUBLICACIONES ****
# *************************************
# p_00
# No es una regla de calidad - Obtiene la lista de las bibliotecas
@carga_namespace.route(
    "/publicacion/p_00",
    doc=False,
    endpoint="p_00",
)
class p_00(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_admin(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas_cargas.get_quality_rule_p_00()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_01
# Publicación con tipo de Datos duplicado
@carga_namespace.route(
    "/publicacion/p_01",
    doc=False,
    endpoint="p_01",
)
class p_01(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_admin(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas_cargas.get_quality_rule_p_01()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_02
# Publicación con tipo de Identificadores duplicado
@carga_namespace.route(
    "/publicacion/p_02",
    doc=False,
    endpoint="p_02",
)
class p_02(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_admin(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas_cargas.get_quality_rule_p_02()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_03
# Autores duplicados en publicación con mismo rol
@carga_namespace.route(
    "/publicacion/p_03",
    doc=False,
    endpoint="p_03",
)
class p_03(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_admin(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas_cargas.get_quality_rule_p_03()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_04
# Publicación sin autores US
@carga_namespace.route(
    "/publicacion/p_04",
    doc=False,
    endpoint="p_04",
)
class p_04(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_admin(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas_cargas.get_quality_rule_p_04()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500
