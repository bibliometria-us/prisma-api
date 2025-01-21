from flask_restx import Namespace, Resource
from flask import Response, make_response, request, jsonify
from routes.carga.fuente.metricas.clarivate_journals import iniciar_carga
from routes.carga.publicacion.idus.carga import CargaPublicacionIdus
from routes.carga.publicacion.idus.parser import IdusParser
from routes.carga.publicacion.scopus.parser import ScopusParser
from routes.carga.publicacion.wos.parser import WosParser
from routes.carga.publicacion.crossref.parser import CrossrefParser
from routes.carga.publicacion.zenodo.parser import ZenodoParser
from routes.carga.publicacion.openalex.parser import OpenalexParser
from routes.carga.publicacion.idus.xml_doi import xmlDoiIdus
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


# **** CARGA DE PUBLICACIONES ****
# Parser rellana un objeto de publicación relleno en formato comun,
# para ello la función realiza los siguentes pasos:
#    - Recibe un id
#    - Llamada a la API del origen y devuelve un json con info de la pub
#    - Mapeo del json al objeto generico
#    - Almacena objeto generico en BD


@carga_namespace.route(
    "/publicacion/scopus/", doc=False, endpoint="carga_publicacion_scopus"
)
class CargaPublicacionScopus(Resource):
    def get(self):
        args = request.args

        idScopus = args.get("id", None)

        try:
            parser = ScopusParser(idScopus=idScopus)
            json = parser.datos_carga_publicacion.to_json()
            # TODO: Añadir funcionalidad de carga en BD
            return Response(json, content_type="application/json; charset=utf-8")

        except Exception:
            return {"message": "Error inesperado"}, 500


@carga_namespace.route("/publicacion/wos/", doc=False, endpoint="carga_publicacion_wos")
class CargaPublicacionWos(Resource):
    def get(self):
        args = request.args

        idWos = args.get("id", None)

        try:
            # TODO: ID medline - contemplar ID wos, medline, etc (posiblemente a nivel de API)
            # TODO: contemplar formato correcto de ID (elevar en parser y campturar en main)
            parser = WosParser(idWos=idWos)
            print(parser.datos_carga_publicacion.__str__())
            json = parser.datos_carga_publicacion.to_json()
            # TODO: Añadir funcionalidad de carga en BD
            return Response(json, content_type="application/json; charset=utf-8")

        except Exception:
            return {"message": "Error inesperado"}, 500


@carga_namespace.route(
    "/publicacion/crossref/", doc=False, endpoint="carga_publicacion_crossref"
)
class CargaPublicacionCrossref(Resource):
    def get(self):
        args = request.args

        idCrossref = args.get("id", None)

        try:
            parser = CrossrefParser(idCrossref=idCrossref)
            print(parser.datos_carga_publicacion.to_dict())
            json = parser.datos_carga_publicacion.to_json()
            # TODO: Añadir funcionalidad de carga en BD
            return Response(json, content_type="application/json; charset=utf-8")

        except Exception:
            return {"message": "Error inesperado"}, 500


@carga_namespace.route(
    "/publicacion/zenodo/", doc=False, endpoint="carga_publicacion_zenodo"
)
class CargaPublicacionZenodo(Resource):
    def get(self):
        args = request.args

        idZenodo = args.get("id", None)

        try:
            parser = ZenodoParser(idZenodo=idZenodo)
            print(parser.datos_carga_publicacion.to_dict())
            json = parser.datos_carga_publicacion.to_json()
            # TODO: Añadir funcionalidad de carga en BD
            return Response(json, content_type="application/json; charset=utf-8")

        except Exception:
            return {"message": "Error inesperado"}, 500


@carga_namespace.route(
    "/publicacion/openalex/", doc=False, endpoint="carga_publicacion_openalex"
)
class CargaPublicacionOpenalex(Resource):
    def get(self):
        args = request.args

        idOpenalex = args.get("id", None)

        try:
            parser = OpenalexParser(idOpenalex=idOpenalex)
            print(parser.datos_carga_publicacion.to_dict())
            json = parser.datos_carga_publicacion.to_json()
            # TODO: Añadir funcionalidad de carga en BD
            return Response(json, content_type="application/json; charset=utf-8")

        except Exception:
            return {"message": "Error inesperado"}, 500


@carga_namespace.route(
    "/publicacion/importar/", doc=False, endpoint="carga_publicacion_importar"
)
class CargaPublicacionImportar(Resource):
    def get(self):
        doi_regex = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"
        wos_regex = r"^WOS:\d{15}$"
        pubmed_regex = r""
        scopus_regex = r"^2-s2\.0-\d{11}$"
        zenodo_regex = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"
        openalex_regex = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"

        args = request.args

        tipo = args.get("tipo", None)
        id = args.get("id", None)

        parser = None
        try:
            match tipo:
                case "doi":
                    # Controla que es un formato doi correcto
                    if not re.match(doi_regex, id, re.IGNORECASE):
                        raise ValueError(f"'{id}' no tiene un formato válido de DOI.")
                    else:
                        # Busqueda en WOS
                        parser = WosParser(idWos=id)
                        # TODO: si no devuelve vacio, guardar en BD
                        # Busqueda en Scopus
                        parser = ScopusParser(idScopus=id)
                        # TODO: si no devuelve vacio, guardar en BD
                        # Busqueda en Idus
                        # TODO: si no devuelve vacio, guardar en BD
                        # Busqueda en CrossRef
                        parser = CrossrefParser(idCrossref=id)
                        # TODO: si no devuelve vacio, guardar en BD
                        # Busqueda en OpenAlex
                        parser = OpenalexParser(idOpenalex=id)
                        # TODO: si no devuelve vacio, guardar en BD
                        # Busqueda en Zenodo
                        parser = ZenodoParser(idZenodo=id)
                        # TODO: si no devuelve vacio, guardar en BD

                case "pubmedId" | "wosId":
                    if not re.match(wos_regex, id, re.IGNORECASE):
                        raise ValueError(
                            f"'{id}' no tiene un formato válido de identificador WOS/PubMed."
                        )
                    parser = WosParser(idWos=id)
                    return "Opción wos seleccionada"
                case "scopusId":
                    if not re.match(scopus_regex, id, re.IGNORECASE):
                        raise ValueError(
                            f"'{id}' no tiene un formato válido de identificador Scopus."
                        )
                    parser = ScopusParser(idScopus=id)
                    return "Opción scopus seleccionada"
                # case "handler":
                #     if not re.match(handler_regex, id, re.IGNORECASE):
                #         raise ValueError(f"'{id}' no tiene un formato válido de DOI.")
                #     return "Opción idus seleccionada"
                case "openalexId":
                    if not re.match(openalex_regex, id, re.IGNORECASE):
                        raise ValueError(
                            f"'{id}' no tiene un formato válido de identificacor OpenAlex."
                        )
                    parser = OpenalexParser(idOpenalex=id)
                    return "Opción openalex seleccionada"
                case "zenodo":
                    if not re.match(zenodo_regex, id, re.IGNORECASE):
                        raise ValueError(
                            f"'{id}' no tiene un formato válido de identificador Zenodo."
                        )
                    parser = ZenodoParser(idZenodo=id)
                    return "Opción zenodo seleccionada"

        except ValueError as e:
            return {"message": e}, 500
        except Exception:
            return {"message": "Error inesperado"}, 500
