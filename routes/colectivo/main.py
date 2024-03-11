import io
from flask_restx import Namespace, Resource
from flask import request, jsonify
from routes.carga.fuente.metricas.clarivate_journals import iniciar_carga
from routes.colectivo.instituciones.carga_instituciones import cargar_instituciones
from security.check_users import es_admin
from celery import current_app
import csv

from utils.format import flask_csv_to_matix, table_to_pandas

colectivo_namespace = Namespace("colectivo", doc=False)


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
