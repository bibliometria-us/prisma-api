from flask_restx import Namespace, Resource
from flask import request, jsonify
from routes.carga.fuente.metricas.clarivate_journals import iniciar_carga
from security.check_users import es_admin
import base64
from celery import current_app, group, chain
from routes.carga.investigador.grupos.actualizar_sica import (
    actualizar_tabla_sica,
    actualizar_grupos_sica,
)
from datetime import datetime

carga_namespace = Namespace("carga", doc=False)


@carga_namespace.route("/investigador/grupos/", doc=False, endpoint="carga_grupos")
class CargaGrupos(Resource):
    def post(self):
        if not es_admin():
            return {"message": "No autorizado"}, 401
        if "files[]" not in request.files:
            return {"error": "No se han encontrado archivos en la petición"}, 400

        files = request.files.getlist("files[]")

        tareas_tablas_sica = []
        for file in files:
            file_path = "/var/www/prisma-api/temp/" + file.filename
            file.save(file_path)
            tareas_tablas_sica.append(
                current_app.tasks["actualizar_tabla_sica"].s(file_path)
            )

        chain(
            group(*tareas_tablas_sica), current_app.tasks["actualizar_grupos_sica"].s()
        ).apply_async()

        return None


@carga_namespace.route(
    "/fuente/wos_journals/", doc=False, endpoint="carga_wos_journals"
)
class CargaWosJournals(Resource):
    def get(self):
        if not es_admin():
            return {"message": "No autorizado"}, 401

        args = request.args
        current_year = datetime.now().year

        fuentes = args.get("fuentes", "todas")
        inicio = int(args.get("inicio", current_year - 1))
        fin = int(args.get("fin", current_year - 1))

        result = iniciar_carga(fuentes, inicio, fin)

        return {"message": result}, 200


@carga_namespace.route(
    "/investigador/colectivos/", doc=False, endpoint="carga_colectivos"
)
class CargaColectivos(Resource):
    def post(self):
        if not es_admin():
            return {"message": "No autorizado"}, 401
        if "files[]" not in request.files:
            return {"error": "No se han encontrado archivos en la petición"}, 400

        args = request.args

        files = request.files.getlist("files[]")
        tipo = args.get("tipo", None)
        acronimo = args.get("acronimo", None)

        return None
