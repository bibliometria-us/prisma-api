from flask_restx import Namespace, Resource
from flask import request, jsonify

from routes.carga.investigador.grupos.actualizar_sica import actualizar_sica
from security.check_users import es_admin

carga_namespace = Namespace(
    'carga', doc = False)

@carga_namespace.route('/investigador/grupos', doc = False)
class CargaGrupos(Resource):
    def post(self):
        if not es_admin():
            return {'message': 'No autorizado'}, 401
        if 'files[]' not in request.files:
            return {'error': "No se han encontrado archivos en la petición"}, 400
        
        files = request.files.getlist('files[]')

        actualizar_sica(files)
        
