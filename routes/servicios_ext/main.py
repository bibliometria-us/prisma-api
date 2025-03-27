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
# ************** BASICOS **************
# *************************************
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
            incidencias = consultas.get_centros()
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
# **** QUALITY RULES PUBLICACIONES ****
# *************************************
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
            incidencias = consultas.get_quality_rule_p_01()
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


# # p_02
# # Publicación con tipo de Identificadores duplicado
# @servicios_ext_namespace.route(
#     "/qr/p_02",
#     doc=False,
#     endpoint="p_02",
# )
# class p_02(Resource):
#     def get(self):
#         args = request.args
#         api_key = args.get("api_key")

#         if not es_admin(api_key=api_key):
#             return {"message": "No autorizado"}, 401
#         try:
#             incidencias = consultas.get_quality_rule_p_02()
#             json = dataframe_to_json(incidencias, orient="records")
#             response = response = make_response(json)
#             response.headers["Content-Type"] = "application/json"

#             return response

#         except ValueError as e:
#             return {"message": str(e)}, 402
#         except Exception as e:
#             return {"message": "Error inesperado"}, 500


# # p_03
# # Autores duplicados en publicación con mismo rol
# @servicios_ext_namespace.route(
#     "/qr/p_03",
#     doc=False,
#     endpoint="p_03",
# )
# class p_03(Resource):
#     def get(self):
#         args = request.args
#         api_key = args.get("api_key")

#         if not es_admin(api_key=api_key):
#             return {"message": "No autorizado"}, 401
#         try:
#             incidencias = consultas.get_quality_rule_p_03()
#             json = dataframe_to_json(incidencias, orient="records")
#             response = response = make_response(json)
#             response.headers["Content-Type"] = "application/json"

#             return response

#         except ValueError as e:
#             return {"message": str(e)}, 402
#         except Exception as e:
#             return {"message": "Error inesperado"}, 500


# # p_04
# # Publicación sin autores US
# @servicios_ext_namespace.route(
#     "/qr/p_04",
#     doc=False,
#     endpoint="p_04",
# )
# class p_04(Resource):
#     def get(self):
#         args = request.args
#         api_key = args.get("api_key")

#         if not es_admin(api_key=api_key):
#             return {"message": "No autorizado"}, 401
#         try:
#             incidencias = consultas.get_quality_rule_p_04()
#             json = dataframe_to_json(incidencias, orient="records")
#             response = response = make_response(json)
#             response.headers["Content-Type"] = "application/json"

#             return response

#         except ValueError as e:
#             return {"message": str(e)}, 402
#         except Exception as e:
#             return {"message": "Error inesperado"}, 500
