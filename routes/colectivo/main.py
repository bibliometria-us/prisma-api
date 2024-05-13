import io
from flask_restx import Namespace, Resource
from flask import request, jsonify
from models.colectivo.centro_mixto import CentroMixto
from models.colectivo.colectivo import Colectivo
from models.colectivo.exceptions.exceptions import (
    LimitePalabrasClave,
    LineaInvestigacionDuplicada,
    PalabraClaveDuplicada,
)
from models.colectivo.instituto import Instituto
from models.colectivo.unidad_excelencia import UnidadExcelencia
from models.colectivo.grupo import Grupo
from routes.carga.fuente.metricas.clarivate_journals import iniciar_carga
from routes.colectivo.instituciones.carga_instituciones import cargar_instituciones
from security.check_users import es_admin, es_editor, pertenece_a_conjunto
from celery import current_app
import csv

from utils.format import flask_csv_to_matix, table_to_pandas

colectivo_namespace = Namespace("colectivo", doc=False)

tipo_colectivo_to_type = {
    "unidadexcelencia": UnidadExcelencia,
    "centromixto": CentroMixto,
    "instituto": Instituto,
    "grupo": Grupo,
}


@colectivo_namespace.route(
    "/institucion", doc=False, endpoint="carga_institucion_colectivos"
)
class CargaGrupos(Resource):
    def post(self):
        if not es_admin():
            return {"message": "No autorizado"}, 401
        if "files[]" not in request.files:
            return {"error": "No se han encontrado archivos en la petición"}, 400

        files = request.files.getlist("files[]")

        file = files[0]

        data = flask_csv_to_matix(file)

        try:
            cargar_instituciones(data)
        except Exception as e:
            return {"message": "error"}, 500

        return {"message": "Carga finalizada correctamente"}, 200


@colectivo_namespace.route("", doc=False)
class ColectivoResource(Resource):

    def post(self):
        args = request.args

        tipo_colectivo = args.get("tipo_colectivo")
        id_colectivo = args.get("id_colectivo")

        if not (
            es_admin()
            | es_editor()
            | pertenece_a_conjunto(tipo_colectivo, id_colectivo, privileged=True)
        ):
            return {"message": "No autorizado"}, 401

        colectivo: Colectivo = tipo_colectivo_to_type[tipo_colectivo]()

        # Obtiene todos los parámetros mapeando los nombres de las columnas y buscando atributos con ese nombre en la petición
        column_args = {
            column: args.get(column, None)
            for column in colectivo.get_editable_columns()
            if args.get(column, None)
        }

        colectivo.get_primary_key().value = id_colectivo

        try:
            colectivo.get()
            colectivo.update_attributes(column_args)
            return {
                "message": "success",
            }, 200

        except Exception as e:
            return {"message": "error"}, 500


@colectivo_namespace.route("/palabraclave", doc=False)
class PalabraClaveColectivo(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    def post(self):

        args = request.args

        tipo_colectivo = args.get("tipo_colectivo")
        id_palabra_clave = int(args.get("id_palabra_clave", None) or 0)
        nombre_palabra_clave = args.get("nombre_palabra_clave", None)
        id_colectivo = args.get("id_colectivo")

        if not (
            es_admin()
            | es_editor()
            | pertenece_a_conjunto(tipo_colectivo, id_colectivo, privileged=True)
        ):
            return {"message": "No autorizado"}, 401

        try:
            colectivo: Colectivo = tipo_colectivo_to_type[tipo_colectivo]()

            colectivo.get_primary_key().value = id_colectivo
            palabra_clave = colectivo.add_palabra_clave(
                id_palabra_clave=id_palabra_clave,
                nombre_palabra_clave=nombre_palabra_clave,
            )

            return {
                "message": "success",
                "id_palabra_clave": palabra_clave.get_primary_key().value,
            }, 200
        except LimitePalabrasClave as e:
            return {
                "error": "limite",
                "message": e.message,
                "max": e.max_palabras_clave,
            }, 403
        except PalabraClaveDuplicada as e:
            return {
                "error": "duplicada",
                "message": e.message,
            }, 403
        except Exception:
            return {"message": "Error inesperado"}, 500

    def delete(self):

        args = request.args

        tipo_colectivo = args.get("tipo_colectivo")
        id_palabra_clave = int(args.get("id_palabra_clave", None) or 0)
        id_colectivo = args.get("id_colectivo")

        if not (
            es_admin()
            | es_editor()
            | pertenece_a_conjunto(tipo_colectivo, id_colectivo, privileged=True)
        ):
            return {"message": "No autorizado"}, 401

        try:
            colectivo: Colectivo = tipo_colectivo_to_type[tipo_colectivo]()

            colectivo.get_primary_key().value = id_colectivo
            colectivo.remove_palabra_clave(id_palabra_clave=id_palabra_clave)

            if colectivo.db.rowcount != 1:
                raise Exception
        except Exception:
            return {"message": "error"}, 500

        return {"message": "success"}, 200


@colectivo_namespace.route("/lineainvestigacion", doc=False)
class LineaInvestigacionColectivo(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    def post(self):

        args = request.args

        tipo_colectivo = args.get("tipo_colectivo")
        id_linea_investigacion = int(args.get("id_linea_investigacion", None) or 0)
        nombre_linea_investigacion = args.get("nombre_linea_investigacion", None)
        id_colectivo = args.get("id_colectivo")

        if not (
            es_admin()
            | es_editor()
            | pertenece_a_conjunto(tipo_colectivo, id_colectivo, privileged=True)
        ):
            return {"message": "No autorizado"}, 401

        try:
            colectivo = tipo_colectivo_to_type[tipo_colectivo]()

            colectivo.get_primary_key().value = id_colectivo
            linea_investigacion = colectivo.add_linea_investigacion(
                id_linea_investigacion=id_linea_investigacion,
                nombre_linea_investigacion=nombre_linea_investigacion,
            )

            return {
                "message": "success",
                "id_linea_investigacion": linea_investigacion.get_primary_key().value,
            }, 200
        except LineaInvestigacionDuplicada as e:
            return {
                "error": "duplicada",
                "message": e.message,
            }, 403
        except Exception:
            return {"message": "Error inesperado"}, 500

    def delete(self):

        args = request.args

        tipo_colectivo = args.get("tipo_colectivo")
        id_linea_investigacion = int(args.get("id_linea_investigacion", None) or 0)
        id_colectivo = args.get("id_colectivo")

        if not (
            es_admin()
            | es_editor()
            | pertenece_a_conjunto(tipo_colectivo, id_colectivo, privileged=True)
        ):
            return {"message": "No autorizado"}, 401

        try:
            colectivo = tipo_colectivo_to_type[tipo_colectivo]()

            colectivo.get_primary_key().value = id_colectivo
            colectivo.remove_linea_investigacion(
                id_linea_investigacion=id_linea_investigacion
            )

            if colectivo.db.rowcount != 1:
                raise Exception
        except Exception:
            return {"message": "error"}, 500

        return {"message": "success"}, 200
