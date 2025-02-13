from flask_restx import Namespace, Resource
from flask import Response, make_response, request, jsonify
from routes.carga.fuente.metricas.clarivate_journals import iniciar_carga
from routes.carga.investigador.centros_censo.carga import carga_centros_censados
from routes.carga.publicacion.idus.parser import IdusParser
from routes.carga.publicacion.scopus.parser import ScopusParser
from routes.carga.investigador.centros_censo.procesado import procesado_fichero
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
from logger.async_request import AsyncRequest
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


# # **** CARGA DE PUBLICACIONES ****
# # Parser rellana un objeto de publicación relleno en formato comun,
# # para ello la función realiza los siguentes pasos:
# #    - Recibe un id
# #    - Llamada a la API del origen y devuelve un json con info de la pub
# #    - Mapeo del json al objeto generico
# #    - Almacena objeto generico en BD


# @carga_namespace.route(
#     "/publicacion/scopus/", doc=False, endpoint="carga_publicacion_scopus"
# )
# class CargaPublicacionScopus_old(Resource):
#     def get(self):
#         args = request.args

#         idScopus = args.get("id", None)

#         try:
#             parser = ScopusParser(idScopus=idScopus)
#             json = parser.datos_carga_publicacion.to_json()
#             # TODO: Añadir funcionalidad de carga en BD
#             return Response(json, content_type="application/json; charset=utf-8")

#         except Exception:
#             return {"message": "Error inesperado"}, 500


# @carga_namespace.route("/publicacion/wos/", doc=False, endpoint="carga_publicacion_wos")
# class CargaPublicacionWos(Resource):
#     def get(self):
#         args = request.args

#         idWos = args.get("id", None)

#         try:
#             # TODO: ID medline - contemplar ID wos, medline, etc (posiblemente a nivel de API)
#             # TODO: contemplar formato correcto de ID (elevar en parser y campturar en main)
#             parser = WosParser(idWos=idWos)
#             print(parser.datos_carga_publicacion.__str__())
#             json = parser.datos_carga_publicacion.to_json()
#             # TODO: Añadir funcionalidad de carga en BD
#             return Response(json, content_type="application/json; charset=utf-8")

#         except Exception:
#             return {"message": "Error inesperado"}, 500


# @carga_namespace.route(
#     "/publicacion/crossref/", doc=False, endpoint="carga_publicacion_crossref"
# )
# class CargaPublicacionCrossref(Resource):
#     def get(self):
#         args = request.args

#         idCrossref = args.get("id", None)

#         try:
#             parser = CrossrefParser(idCrossref=idCrossref)
#             print(parser.datos_carga_publicacion.to_dict())
#             json = parser.datos_carga_publicacion.to_json()
#             # TODO: Añadir funcionalidad de carga en BD
#             return Response(json, content_type="application/json; charset=utf-8")

#         except Exception:
#             return {"message": "Error inesperado"}, 500


# @carga_namespace.route(
#     "/publicacion/zenodo/", doc=False, endpoint="carga_publicacion_zenodo"
# )
# class CargaPublicacionZenodo(Resource):
#     def get(self):
#         args = request.args

#         idZenodo = args.get("id", None)

#         try:
#             parser = ZenodoParser(idZenodo=idZenodo)
#             print(parser.datos_carga_publicacion.to_dict())
#             json = parser.datos_carga_publicacion.to_json()
#             # TODO: Añadir funcionalidad de carga en BD
#             return Response(json, content_type="application/json; charset=utf-8")

#         except Exception:
#             return {"message": "Error inesperado"}, 500


# @carga_namespace.route(
#     "/publicacion/openalex/", doc=False, endpoint="carga_publicacion_openalex"
# )
# class CargaPublicacionOpenalex(Resource):
#     def get(self):
#         args = request.args

#         idOpenalex = args.get("id", None)

#         try:
#             parser = OpenalexParser(idOpenalex=idOpenalex)
#             print(parser.datos_carga_publicacion.to_dict())
#             json = parser.datos_carga_publicacion.to_json()
#             # TODO: Añadir funcionalidad de carga en BD
#             return Response(json, content_type="application/json; charset=utf-8")

#         except Exception:
#             return {"message": "Error inesperado"}, 500


# # obsoleto
# @carga_namespace.route(
#     "/publicacion/importar_obs/", doc=False, endpoint="carga_publicacion_importar_obs"
# )
# class CargaPublicacionImportar(Resource):
#     def get(self):
#         doi_regex = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"
#         wos_regex = r"^WOS:\d{15}$"
#         pubmed_regex = r""
#         scopus_regex = r"^2-s2\.0-\d{11}$"
#         zenodo_regex = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"
#         openalex_regex = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"

#         args = request.args

#         tipo = args.get("tipo", None)
#         id = args.get("id", None)

#         parser = None
#         try:
#             match tipo:
#                 case "doi":
#                     # Controla que es un formato doi correcto
#                     if not re.match(doi_regex, id, re.IGNORECASE):
#                         raise ValueError(f"'{id}' no tiene un formato válido de DOI.")
#                     else:
#                         # Busqueda en WOS
#                         parser = WosParser(idWos=id)
#                         # TODO: si no devuelve vacio, guardar en BD
#                         # Busqueda en Scopus
#                         parser = ScopusParser(idScopus=id)
#                         # TODO: si no devuelve vacio, guardar en BD
#                         # Busqueda en Idus
#                         # TODO: si no devuelve vacio, guardar en BD
#                         # Busqueda en CrossRef
#                         parser = CrossrefParser(idCrossref=id)
#                         # TODO: si no devuelve vacio, guardar en BD
#                         # Busqueda en OpenAlex
#                         parser = OpenalexParser(idOpenalex=id)
#                         # TODO: si no devuelve vacio, guardar en BD
#                         # Busqueda en Zenodo
#                         parser = ZenodoParser(idZenodo=id)
#                         # TODO: si no devuelve vacio, guardar en BD

#                 case "pubmedId" | "wosId":
#                     if not re.match(wos_regex, id, re.IGNORECASE):
#                         raise ValueError(
#                             f"'{id}' no tiene un formato válido de identificador WOS/PubMed."
#                         )
#                     parser = WosParser(idWos=id)
#                     return "Opción wos seleccionada"
#                 case "scopusId":
#                     if not re.match(scopus_regex, id, re.IGNORECASE):
#                         raise ValueError(
#                             f"'{id}' no tiene un formato válido de identificador Scopus."
#                         )
#                     parser = ScopusParser(idScopus=id)
#                     return "Opción scopus seleccionada"
#                 # case "handler":
#                 #     if not re.match(handler_regex, id, re.IGNORECASE):
#                 #         raise ValueError(f"'{id}' no tiene un formato válido de DOI.")
#                 #     return "Opción idus seleccionada"
#                 case "openalexId":
#                     if not re.match(openalex_regex, id, re.IGNORECASE):
#                         raise ValueError(
#                             f"'{id}' no tiene un formato válido de identificacor OpenAlex."
#                         )
#                     parser = OpenalexParser(idOpenalex=id)
#                     return "Opción openalex seleccionada"
#                 case "zenodo":
#                     if not re.match(zenodo_regex, id, re.IGNORECASE):
#                         raise ValueError(
#                             f"'{id}' no tiene un formato válido de identificador Zenodo."
#                         )
#                     parser = ZenodoParser(idZenodo=id)
#                     return "Opción zenodo seleccionada"

#         except ValueError as e:
#             return {"message": e}, 500
#         except Exception:
#             return {"message": "Error inesperado"}, 500


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
                    CargaPublicacionScopus().carga_publicacion(tipo=tipo, id=id)
                    # CargaPublicacionWos().carga_publicacion(tipo=tipo, id=id)
                    # CargaPublicacionOpenalex().carga_publicacion(tipo=tipo, id=id)
                    # CargaPublicacionZenodo().carga_publicacion(tipo=tipo, id=id)
                    # CargaPublicacionCrossref().carga_publicacion(tipo=tipo, id=id)
                # TODO: QUEDA IDUS

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# @carga_namespace.route(
#     "/publicacion/importar_publicaciones_investigador/",
#     doc=False,
#     endpoint="carga_publicaciones_investigador_",
# )
# class CargaPublicacionImportar_(Resource):
#     def get(self):
#         args = request.args
#         tipo = args.get("tipo", None)
#         id = args.get("id", None)

#         # TODO: llamar a carga unit BD pasandole los params

#         pass


# @carga_namespace.route(
#     "/publicacion/importar_publicaciones_masivo/",
#     doc=False,
#     endpoint="carga_publicaciones_masivo_2",
# )
# class CargaPublicacionImportar_2(Resource):
#     def get(self):
#         args = request.args
#         tipo = args.get("tipo", None)
#         id = args.get("id", None)

#         # TODO: llamar a carga unit BD pasandole los params

#         pass
