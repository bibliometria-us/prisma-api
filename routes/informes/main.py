from flask_restx import Namespace, Resource
from flask import jsonify, request, session, redirect, url_for, send_file, Response
import config.global_config as gconfig
from logger.async_request import AsyncRequest
from routes.informes.pub_metrica.pub_metrica import (
    generar_informe,
    buscar_publicaciones,
)
from datetime import datetime, date
import os

from db.conexion import BaseDatos
from routes.informes.misc.misc import get_metrica_calidad

from routes.informes.pub_metrica.security import comprobar_permisos
from security.check_users import es_admin, pertenece_a_departamento

from utils import format
from celery import current_app

informe_namespace = Namespace("informe", description="Consultas para obtener informes")

global_responses = gconfig.responses

global_params = gconfig.params


@informe_namespace.route("/pub_metrica/", endpoint="pub_metrica")
class InformePubMetrica(Resource):
    @informe_namespace.doc(
        responses=global_responses,
        params={
            "salida": {
                "name": "salida",
                "description": "Formato de salida. Especificar en este campo en caso de que no pueda hacerlo mediante el header de la petición",
                "type": "string",
                "enum": ["pdf", "excel"],
            },
            "departamento": {
                "name": "departamento",
                "description": "ID del departamento del que obtener el informe",
                "type": "string",
            },
            "grupo": {
                "name": "grupo",
                "description": "ID del grupo del que obtener el informe",
                "type": "string",
            },
            "instituto": {
                "name": "instituto",
                "description": "ID del instituto del que obtener el informe",
                "type": "string",
            },
            "centro_mixto": {
                "name": "centro_mixto",
                "description": "ID del centro mixto del que obtener el informe",
                "type": "string",
            },
            "unidad_excelencia": {
                "name": "unidad_excelencia",
                "description": "ID de la unidad de excelencia de la que obtener el informe",
                "type": "string",
            },
            "centro": {
                "name": "centro",
                "description": "ID del centro del que obtener el informe",
                "type": "string",
            },
            "doctorado": {
                "name": "doctorado",
                "description": "ID del programa de doctorado del que obtener el informe",
                "type": "string",
            },
            "area": {
                "name": "centro",
                "description": "ID del área de conocimiento de la que obtener el informe",
                "type": "string",
            },
            "investigadores": {
                "name": "investigadores",
                "description": "Lista de ID de investigadores",
                "type": "str",
            },
            "inicio": {
                "name": "inicio",
                "description": "Año de inicio",
                "type": "int",
            },
            "fin": {
                "name": "fin",
                "description": "Año de fin",
                "type": "int",
            },
        },
    )
    def get(self):

        args = {}

        for key, value in request.args.to_dict().items():
            if value != "":
                args[key] = value

        headers = request.headers

        tipo = args.get("salida", None)
        api_key = args.get("api_key", None)
        # Almacenar fuentes directamente en su diccionario
        fuentes = {
            "departamento": args.get("departamento", None),
            "grupo": args.get("grupo", None),
            "instituto": args.get("instituto", None),
            "centro_mixto": args.get("centro_mixto", None)
            or args.get("centromixto", None),
            "unidad_excelencia": args.get("unidad_excelencia", None)
            or args.get("unidadexcelencia", None),
            "investigadores": args.get("investigadores", None),
            "investigador": args.get("investigador"),
            "centro": args.get("centro", None),
            "centrocenso": args.get("centrocenso", None),
            "area": args.get("area", None),
            "doctorado": args.get("doctorado", None),
        }

        try:
            comprobar_permisos(fuentes, api_key=api_key)
        except:
            return {"message": "No autorizado"}, 401

        # Convertir contenido de fuentes en listas
        fuentes = {
            tipo_fuente: list(str(valor).split(","))
            for tipo_fuente, valor in fuentes.items()
            if valor is not None
        }
        año_inicio = int(args.get("inicio", datetime.now().year))
        año_fin = int(args.get("fin", datetime.now().year))

        send_email = headers.get("EMAIL", False)

        timestamp = datetime.now().strftime("%d%m%Y_%H%M")
        tipo_salida_to_format = {
            "pdf": "pdf",
            "excel": "xlsx",
        }

        if len(fuentes) > 1 or len(list(fuentes.values())[0]) > 1:
            base_filename = f"informe_personalizado_{timestamp}"
        else:
            base_filename = f"informe_{list(fuentes.keys())[0]}_{','.join(list(fuentes.values())[0])}_{timestamp}"
        download_filename = f"{base_filename}.{tipo_salida_to_format[tipo]}"
        internal_filename = f"temp/{download_filename}"

        if not send_email:
            try:
                generar_informe(
                    fuentes, año_inicio, año_fin, tipo, f"temp/{base_filename}"
                )
            except Exception as e:
                return {"error": e.message}, 400

            response = send_file(
                internal_filename, as_attachment=True, download_name=download_filename
            )

            os.remove(internal_filename)

            return response

        else:
            try:

                params = {
                    "año_inicio": año_inicio,
                    "año_fin": año_fin,
                    "fuentes": fuentes,
                    "tipo": tipo,
                }

                buscar_publicaciones(fuentes, año_inicio, año_fin)

                destinatarios = session["samlUserdata"]["mail"]
                async_request = AsyncRequest(
                    request_type="pub_metrica",
                    email=destinatarios[-1],
                    params=str(params),
                )

                current_app.tasks["informe_pub_metrica"].apply_async(
                    [
                        fuentes,
                        año_inicio,
                        año_fin,
                        tipo,
                        f"temp/{base_filename}",
                        destinatarios,
                        async_request.id,
                    ]
                )
                return {"message": "Informe solicitado correctamente"}, 200
            except Exception as e:
                return {"error": e.message}, 400


@informe_namespace.route(
    "/medias_departamento/", endpoint="medias_departamento", doc=False
)
class InformeMediasDepartamento(Resource):
    def get(self):
        args = request.args

        departamento = args.get("departamento", None)

        try:
            assert pertenece_a_departamento(departamento) | es_admin()
        except:
            return {"message": "No autorizado"}, 401

        directory_path = "routes/informes/medias_departamento/docs"
        file_names = [
            f
            for f in os.listdir(directory_path)
            if os.path.isfile(os.path.join(directory_path, f))
        ]

        departamentos = []
        for file_name in file_names:
            departamentos.append(file_name.split("_")[1])
            if file_name.split("_")[1] == departamento:
                internal_filename = f"{directory_path}/{file_name}"
                response = send_file(
                    internal_filename, as_attachment=True, download_name=file_name
                )

        departamentos = set(departamentos)
        return response


@informe_namespace.route("/calidad/", endpoint="calidad", doc=False)
class InformeCalidad(Resource):
    def get(self):
        args = request.args
        api_key = args.get("api_key", None)
        try:
            assert es_admin(api_key=api_key)
        except:
            return {"message": "No autorizado"}, 401
        try:
            bd_object = BaseDatos()
            response = get_metrica_calidad(bd=bd_object)
            res = format.dict_to_json(response)
            return Response(res, status=200, mimetype="application/json")
        except Exception as e:
            return {"error": e.message}, 400
