from flask_restx import Namespace, Resource
from flask import request, jsonify
from security.check_users import es_admin
import base64
from celery import current_app, group, chain
from routes.carga.investigador.grupos.actualizar_sica import actualizar_tabla_sica, actualizar_grupos_sica

carga_namespace = Namespace(
    'carga', doc = False)


@carga_namespace.route('/investigador/grupos', doc = False, endpoint = "carga_grupos")
class CargaGrupos(Resource):
    def post(self):
        if not es_admin():
            return {'message': 'No autorizado'}, 401
        if 'files[]' not in request.files:
            return {'error': "No se han encontrado archivos en la petición"}, 400
    
        files = request.files.getlist('files[]')
        
        tareas_tablas_sica = []
        for file in files:
            file_path = '/var/www/prisma-api/temp/' + file.filename
            file.save(file_path)
            tareas_tablas_sica.append(current_app.tasks['actualizar_tabla_sica'].s(file_path))
        
        chain(group(*tareas_tablas_sica), current_app.tasks['actualizar_grupos_sica'].s()).apply_async()

        return None
        
