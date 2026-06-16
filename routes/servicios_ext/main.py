from flask_restx import Namespace, Resource
from flask import Response, make_response, request, jsonify
from models.investigador import Investigador
from routes.servicios_ext import consultas

from security.check_users import es_admin, es_editor, es_visor
from celery import current_app
from datetime import datetime
from logger.async_request import AsyncRequest
import re
from utils.format import dataframe_to_json

servicios_ext_namespace = Namespace("servicios_ext", doc=False)


# *************************************
# ******** FUNCIONES TEMPORALES *******
# *************************************
@servicios_ext_namespace.route(
    "/eliminar_autores_pub",
    doc=False,
    endpoint="eliminar_autores_pub",
)
class eliminar_autores_pub(Resource):
    def get(self):
        args = request.args
        id_pub = args.get("id_pub")
        if not es_admin():
            return {"message": "No autorizado, inicie sesión en Prisma."}, 401
        try:
            consultas.eliminar_autores_pub(id_publicacion=id_pub)
            return {"message": "La acción se ha completado con éxito."}, 200

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# *************************************
# ************** BASICOS **************
# *************************************


# CENTROS
@servicios_ext_namespace.route(
    "/centros",
    doc=False,
    endpoint="lista_centros",
)
class get_centros(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_centros()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# DEPARTAMENTOS
@servicios_ext_namespace.route(
    "/departamentos",
    doc=False,
    endpoint="lista_departamentos",
)
class get_departamentos(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_departamentos()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# ÁREAS
@servicios_ext_namespace.route(
    "/areas",
    doc=False,
    endpoint="lista_areas",
)
class get_areas(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_areas()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# INSTITUTOS
@servicios_ext_namespace.route(
    "/institutos",
    doc=False,
    endpoint="lista_institutos",
)
class get_institutos(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_institutos()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# CENTROS DE EXCELENCIA
@servicios_ext_namespace.route(
    "/centros_exelencia",
    doc=False,
    endpoint="lista_centros_exelencia",
)
class get_centros_exelecia(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_centros_excelencia()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500

# GRUPOS
@servicios_ext_namespace.route(
    "/grupos",
    doc=False,
    endpoint="lista_grupos",
)
class get_grupos(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_grupos()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500

# PROGRAMAS DE DOCTORADO
@servicios_ext_namespace.route(
    "/programas_doctorado",
    doc=False,
    endpoint="lista_programas_doctorado",
)
class get_programas_doctorado(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_programa_doctorado()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# *************************************
# ******** INF. BIBLIOMETRICO *********
# *************************************
@servicios_ext_namespace.route(
    "/investigadores",
    doc=False,
    endpoint="lista_investigadores",
)
class investigadores(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_investigadores()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# *************************************
# ******* REGLAS DE VALIDACION ********
# *************************************


# ************** BASICOS **************
# BIBLIOTECAS
@servicios_ext_namespace.route(
    "/bibliotecas",
    doc=False,
    endpoint="lista_bibliotecas",
)
class get_bibliotecas(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_bibliotecas()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# TIPOS DE PUBLICACION PERMITIDOS
@servicios_ext_namespace.route(
    "/tipos_publicacion_permitidos",
    doc=False,
    endpoint="lista_tipos_publicacion_permitidos",
)
class get_tipos_publicacion_permitidos(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_tipos_publicaciones_permitidos()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500

# TIPOS DE FUENTE PERMITIDOS
@servicios_ext_namespace.route(
    "/tipos_fuente_permitidos",
    doc=False,
    endpoint="lista_tipos_fuente_permitidos",
)
class get_tipos_fuente_permitidos(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_tipos_fuente_permitidos()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# ************ PUBLICACIONES *****************
# Publicaciones con tipos no permitidos
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_con_tipos_no_permitidos",
    doc=False,
    endpoint="pub_publicaciones_con_tipos_no_permitidos",
)
class pub_publicaciones_con_tipos_no_permitidos(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_con_tipos_no_permitidos()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Publicaciones con tipos OTROS
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_con_tipos_otros",
    doc=False,
    endpoint="pub_publicaciones_con_tipos_otros",
)
class pub_publicaciones_con_tipos_otros(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_con_tipos_otros()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Publicaciones con más de un ID del mismo tipo
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_mas_de_un_id_mismo_tipo",
    doc=False,
    endpoint="pub_publicaciones_mas_de_un_id_mismo_tipo",
)
class pub_publicaciones_mas_de_un_id_mismo_tipo(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_mas_de_un_id_mismo_tipo()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Publicaciones sin ID
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_sin_id",
    doc=False,
    endpoint="pub_publicaciones_sin_id",
)
class pub_publicaciones_sin_id(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_sin_id()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Publicaciones con autores repetidos
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_autores_repetidos",
    doc=False,
    endpoint="pub_publicaciones_autores_repetidos",
)
class pub_publicaciones_autores_repetidos(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_autores_repetidos()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones sin fuente
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_sin_fuente",
    doc=False,
    endpoint="pub_publicaciones_sin_fuente",
)
class pub_publicaciones_sin_fuente(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_sin_fuente()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones sin autores US
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_sin_autores_us",
    doc=False,
    endpoint="pub_publicaciones_sin_autores_us",
)
class pub_publicaciones_sin_autores_us(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_sin_autores_us()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


#  Lista de publicaciones sin autores
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_sin_autores",
    doc=False,
    endpoint="pub_publicaciones_sin_autores",
)
class pub_publicaciones_sin_autores(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_sin_autores()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Num Publicaciones por tipo y biblioteca
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_num_publicaciones_por_biblioteca",
    doc=False,
    endpoint="pub_num_publicaciones_por_biblioteca",
)
class pub_num_publicaciones_por_biblioteca(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_num_publicaciones_por_biblioteca()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones repetidas por título, tipo y año
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_repetidas_titulo_tipo_agno",
    doc=False,
    endpoint="pub_publicaciones_repetidas_titulo_tipo_agno",
)
class pub_publicaciones_repetidas_titulo_tipo_agno(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_repetidas_titulo_tipo_agno()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones repetidas por título, tipo, año y fuente
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_repetidas_titulo_tipo_fuente",
    doc=False,
    endpoint="pub_publicaciones_repetidas_titulo_tipo_fuente",
)
class pub_publicaciones_repetidas_titulo_tipo_fuente(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_repetidas_titulo_tipo_fuente()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones de tipo capitulo cuya fuente sea tipo colección
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_tipo_capitulo_fuente_coleccion",
    doc=False,
    endpoint="pub_publicaciones_tipo_capitulo_fuente_coleccion",
)
class pub_publicaciones_tipo_capitulo_fuente_coleccion(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = (
                consultas.get_pub_publicaciones_tipo_capitulo_fuente_coleccion()
            )
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones socias a una fuente eliminada
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_asociadas_a_fuentes_eliminadas",
    doc=False,
    endpoint="pub_publicaciones_asociadas_a_fuentes_eliminadas",
)
class pub_publicaciones_asociadas_a_fuentes_eliminadas(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = (
                consultas.get_pub_publicaciones_asociadas_a_fuentes_eliminadas()
            )
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de 200 ultimas publicaciones insertadas
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_ultimas_200",
    doc=False,
    endpoint="pub_ultimas_200",
)
class pub_ultimas_200(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_ultimas_200()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones con más de un Dato del mismo tipo
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_mas_de_un_dato_mismo_tipo",
    doc=False,
    endpoint="pub_publicaciones_mas_de_un_dato_mismo_tipo",
)
class pub_publicaciones_mas_de_un_dato_mismo_tipo(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_mas_de_un_dato_mismo_tipo()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones con idScopus y que la revista asociada no tenga metrica en SJR/Citiscore
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_scopus_sin_metrica_Revista",
    doc=False,
    endpoint="pub_publicaciones_scopus_sin_metrica_Revista",
)
class pub_publicaciones_scopus_sin_metrica_Revista(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_scopus_sin_metrica_Revista()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones con idWOS y que la revista asociada no tenga metrica en JCR ni JCI
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_wos_sin_metrica_Revista",
    doc=False,
    endpoint="pub_publicaciones_wos_sin_metrica_Revista",
)
class pub_publicaciones_wos_sin_metrica_Revista(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_wos_sin_metrica_Revista()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones asociadas a una Revista con SJR/Citiscore y no tenga idScopus
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_sjr_citescore_sin_scopus",
    doc=False,
    endpoint="pub_publicaciones_sjr_citescore_sin_scopus",
)
class pub_publicaciones_sjr_citescore_sin_scopus(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_sjr_citescore_sin_scopus()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones asociadas a una Revista con JCR y no tenga idWOS
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_publicaciones_jcr_jci_sin_wos",
    doc=False,
    endpoint="pub_publicaciones_jcr_jci_sin_wos",
)
class pub_publicaciones_jcr_jci_sin_wos(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_pub_publicaciones_jcr_jci_sin_wos()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# ****************************************
# ************ INVESTIGADORES *************
# ****************************************


# Lista de investigadores sin email
@servicios_ext_namespace.route(
    "/reglas_validacion/get_inv_investigadores_sin_email",
    doc=False,
    endpoint="inv_investigadores_sin_email",
)
class inv_investigadores_sin_email(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_inv_investigadores_sin_email()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de investigadores sin identificadores
@servicios_ext_namespace.route(
    "/reglas_validacion/get_inv_investigadores_sin_identificadores",
    doc=False,
    endpoint="get_inv_investigadores_sin_identificadores",
)
class get_inv_investigadores_sin_identificadores(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_inv_investigadores_sin_identificadores()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de investigadores sin publicaciones
@servicios_ext_namespace.route(
    "/reglas_validacion/get_inv_investigadores_sin_publicaciones",
    doc=False,
    endpoint="get_inv_investigadores_sin_publicaciones",
)
class get_inv_investigadores_sin_publicaciones(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_inv_investigadores_sin_publicaciones()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de los 20 últimos investigadores insertados
@servicios_ext_namespace.route(
    "/reglas_validacion/get_inv_investigadores_20_ultimos_insertados",
    doc=False,
    endpoint="get_inv_investigadores_20_ultimos_insertados",
)
class get_inv_investigadores_20_ultimos_insertados(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_inv_investigadores_20_ultimos_insertados()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de los investigadores sin ORCID
@servicios_ext_namespace.route(
    "/reglas_validacion/get_inv_investigadores_sin_orcid",
    doc=False,
    endpoint="get_inv_investigadores_sin_orcid",
)
class get_inv_investigadores_sin_orcid(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_inv_investigadores_sin_orcid()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# ****************************************
# ************ FUENTES *************
# ****************************************


@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuentes_sin_identificadores",
    doc=False,
    endpoint="get_fuentes_sin_identificadores",
)
class get_fuentes_sin_identificadores(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_fuentes_sin_identificadores()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuentes_sin_tipo_admitido",
    doc=False,
    endpoint="get_fuentes_sin_tipo_admitido",
)
class get_fuentes_sin_tipo_admitido(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_fuentes_sin_tipo_admitido()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuentes_coleccion_con_issn_y_isbn",
    doc=False,
    endpoint="get_fuentes_coleccion_con_issn_y_isbn",
)
class get_fuentes_coleccion_con_issn_y_isbn(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_fuentes_coleccion_con_issn_y_isbn()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuentes_dato_coleccion_enlazado_a_fuente_no_coleccion",
    doc=False,
    endpoint="get_fuentes_dato_coleccion_enlazado_a_fuente_no_coleccion",
)
class get_fuentes_dato_coleccion_enlazado_a_fuente_no_coleccion(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = (
                consultas.get_fuentes_dato_coleccion_enlazado_a_fuente_no_coleccion()
            )
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuentes_con_coleccion_a_si_misma",
    doc=False,
    endpoint="get_fuentes_con_coleccion_a_si_misma",
)
class get_fuentes_con_coleccion_a_si_misma(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_fuentes_con_coleccion_a_si_misma()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuentes_no_tipo_libro_con_colecciones",
    doc=False,
    endpoint="get_fuentes_no_tipo_libro_con_colecciones",
)
class get_fuentes_no_tipo_libro_con_colecciones(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_fuentes_no_tipo_libro_con_colecciones()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500



@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuente_sin_publicaciones_no_APC",
    doc=False,
    endpoint="get_fuente_sin_publicaciones_no_APC",
)
class get_fuente_sin_publicaciones_no_APC(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_fuente_sin_publicaciones_no_APC()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuente_con_issn_e_isbn",
    doc=False,
    endpoint="get_fuente_con_issn_e_isbn",
)
class get_fuente_con_issn_e_isbn(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_fuente_con_issn_e_isbn()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500

@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuentes_APC_no_activas",
    doc=False,
    endpoint="get_fuentes_APC_no_activas",
)
class get_fuentes_APC_no_activas(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_fuentes_APC_no_activas()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500
        
        
@servicios_ext_namespace.route(
    "/reglas_validacion/get_fuentes_identificador_repetido",
    doc=False,
    endpoint="get_fuentes_identificador_repetido",
)
class get_fuentes_identificador_repetido(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_fuentes_identificador_repetido()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500

# ****************************************
# ************ PROYECTOS *************
# ****************************************


@servicios_ext_namespace.route(
    "/reglas_validacion/get_proyectos_referencia_nula_o_menos_5_caracteres",
    doc=False,
    endpoint="get_proyectos_referencia_nula_o_menos_5_caracteres",
)
class get_proyectos_referencia_nula_o_menos_5_caracteres(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_proyectos_referencia_nula_o_menos_5_caracteres()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_proyectos_importe_nulo_menor_100",
    doc=False,
    endpoint="get_proyectos_importe_nulo_menor_100",
)
class get_proyectos_importe_nulo_menor_100(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_proyectos_importe_nulo_menor_100()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# ****************************************
# ************ FINANCIACION *************
# ****************************************


@servicios_ext_namespace.route(
    "/reglas_validacion/get_financiacion_codigo_nulo_o_menos_4_caracteres",
    doc=False,
    endpoint="get_financiacion_codigo_nulo_o_menos_4_caracteres",
)
class get_financiacion_codigo_nulo_o_menos_4_caracteres(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_financiacion_codigo_nulo_o_menos_4_caracteres()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_financiacion_agencia_nula_o_menos_5_caracteres",
    doc=False,
    endpoint="get_financiacion_agencia_nula_o_menos_5_caracteres",
)
class get_financiacion_agencia_nula_o_menos_5_caracteres(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_financiacion_agencia_nula_o_menos_5_caracteres()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_financiacion_repetida_por_publicacion",
    doc=False,
    endpoint="get_financiacion_repetida_por_publicacion",
)
class get_financiacion_repetida_por_publicacion(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_financiacion_repetida_por_publicacion()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


#--------- Metricas financiacion-------

@servicios_ext_namespace.route(
    "/reglas_validacion/get_num_proyectos_con_financiacion",
    doc=False,
    endpoint="get_num_proyectos_con_financiacion",
)
class get_num_proyectos_con_financiacion(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_num_proyectos_con_financiacion()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


@servicios_ext_namespace.route(
    "/reglas_validacion/get_num_financiacion_con_proyectos",
    doc=False,
    endpoint="get_num_financiacion_con_proyectos",
)
class get_num_financiacion_con_proyectos(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_num_financiacion_con_proyectos()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


#############################################
################# RANKING ###################
#############################################

@servicios_ext_namespace.route(
    "/reglas_validacion/get_listado_metricas_investigadores",
    doc=False,
    endpoint="get_listado_metricas_investigadores",
)
class get_listado_metricas_investigadores(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_listado_metricas_investigadores()
            json = dataframe_to_json(incidencias, orient="records", empty=True)
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500