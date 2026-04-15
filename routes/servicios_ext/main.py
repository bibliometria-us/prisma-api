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
            json = dataframe_to_json(incidencias, orient="records")
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
class get_centros(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_departamentos()
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# *************************************
# ********* QUALITY RULES  ************
# *************************************


# ************ PUBLICACIONES *****************
# p_00
# Publicación con tipo de Datos duplicado
@servicios_ext_namespace.route(
    "/qr/p_00",
    doc=False,
    endpoint="p_00",
)
class p_00(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_00()
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
@servicios_ext_namespace.route(
    "/qr/p_01",
    doc=False,
    endpoint="p_01",
)
class p_01(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_01()
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
@servicios_ext_namespace.route(
    "/qr/p_02",
    doc=False,
    endpoint="p_02",
)
class p_02(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_02()
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
@servicios_ext_namespace.route(
    "/qr/p_03",
    doc=False,
    endpoint="p_03",
)
class p_03(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_03()
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
@servicios_ext_namespace.route(
    "/qr/p_04",
    doc=False,
    endpoint="p_04",
)
class p_04(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_04()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_05
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/p_05",
    doc=False,
    endpoint="p_05",
)
class p_05(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_05()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_06
# Publicación tipo capitulo cuya fuente sea tipo colección
@servicios_ext_namespace.route(
    "/qr/p_06",
    doc=False,
    endpoint="p_06",
)
class p_06(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_06()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_07
# Publicación tipo capitulo cuya fuente sea tipo colección
@servicios_ext_namespace.route(
    "/qr/p_07",
    doc=False,
    endpoint="p_07",
)
class p_07(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_07()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_08
# Publicación tipo capitulo cuya fuente sea tipo colección
@servicios_ext_namespace.route(
    "/qr/p_08",
    doc=False,
    endpoint="p_08",
)
class p_08(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_08()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_15
# Últimas 100 publicaciones insertadas
@servicios_ext_namespace.route(
    "/qr/p_15",
    doc=False,
    endpoint="p_15",
)
class p_15(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_15()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# p_16
# Publicación duplicada por título, año, tipo y fuente
@servicios_ext_namespace.route(
    "/qr/p_16",
    doc=False,
    endpoint="p_16",
)
class p_16(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_p_16()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# *************** FUENTES ************************


# f_01
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_01",
    doc=False,
    endpoint="f_01",
)
class f_01(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_01()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_02
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_02",
    doc=False,
    endpoint="f_02",
)
class f_02(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_02()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_03
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_03",
    doc=False,
    endpoint="f_03",
)
class f_03(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_03()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_04
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_04",
    doc=False,
    endpoint="f_04",
)
class f_04(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_04()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_05
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_05",
    doc=False,
    endpoint="f_05",
)
class f_05(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_05()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_06
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_06",
    doc=False,
    endpoint="f_06",
)
class f_06(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_06()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_07
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_07",
    doc=False,
    endpoint="f_07",
)
class f_07(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_07()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_08
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_08",
    doc=False,
    endpoint="f_08",
)
class f_08(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_08()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_09
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_09",
    doc=False,
    endpoint="f_09",
)
class f_09(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_09()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_10
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_10",
    doc=False,
    endpoint="f_10",
)
class f_10(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_10()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_11
# Publicaciones sin identificadores
@servicios_ext_namespace.route(
    "/qr/f_11",
    doc=False,
    endpoint="f_11",
)
class f_11(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_11()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# f_16
# Fuentes con títulos vacíos
@servicios_ext_namespace.route(
    "/qr/f_16",
    doc=False,
    endpoint="f_16",
)
class f_16(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_f_16()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# *************************************
# ****** INVESTIGADORES ***************
# *************************************
# i_02
# 50 ultimos investigadores
@servicios_ext_namespace.route(
    "/qr/i_02",
    doc=False,
    endpoint="i_02",
)
class f_11(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key")

        if not es_visor(api_key=api_key):
            return {"message": "No autorizado"}, 401
        try:
            incidencias = consultas.get_quality_rule_i_02()
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones asociadas a una Revista con SJR/Citiscore y no tenga idScopus
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_Publicaciones_sjr_citescore_sin_scopus",
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
            incidencias = consultas.get_pub_Publicaciones_sjr_citescore_sin_scopus()
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500


# Lista de publicaciones asociadas a una Revista con JCR y no tenga idWOS
@servicios_ext_namespace.route(
    "/reglas_validacion/pub_Publicaciones_jcr_jci_sin_wos",
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
            incidencias = consultas.get_pub_Publicaciones_jcr_jci_sin_wos()
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
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
            json = dataframe_to_json(incidencias, orient="records")
            response = response = make_response(json)
            response.headers["Content-Type"] = "application/json"

            return response

        except ValueError as e:
            return {"message": str(e)}, 402
        except Exception as e:
            return {"message": "Error inesperado"}, 500
