from flask_restx import Namespace, Resource
from flask import request, session, redirect, url_for, send_file
import config.global_config as gconfig
from routes.informes.pub_metrica.pub_metrica import generar_informe
from datetime import datetime
import os
informe_namespace = Namespace(
    'informe', description="Consultas para obtener informes")

global_responses = gconfig.responses

global_params = gconfig.params


@informe_namespace.route('/pub_metrica/')
class InformePubMetrica(Resource):
    @informe_namespace.doc(
        responses=global_responses,
        params={
            'salida': {
                'name': 'salida',
                'description': 'Formato de salida. Especificar en este campo en caso de que no pueda hacerlo mediante el header de la petición',
                'type': 'string',
                'enum': ["pdf", "xlsx"]
            },
            'departamento': {
                'name': 'departamento',
                'description': 'ID del departamento del que obtener el informe',
                'type': 'string',
            },
            'grupo': {
                'name': 'grupo',
                'description': 'ID del grupo del que obtener el informe',
                'type': 'string',
            },
            'instituto': {
                'name': 'instituto',
                'description': 'ID del instituto del que obtener el informe',
                'type': 'string',
            },
            'inicio': {
                'name': 'inicio',
                'description': 'Año de inicio',
                'type': 'int',
            },
            'fin': {
                'name': 'fin',
                'description': 'Año de fin',
                'type': 'int',
            }, }
    )
    def get(self):
        args = request.args

        tipo = args.get('salida', None)
        # Almacenar fuentes directamente en su diccionario
        fuentes = {
            "departamento": args.get('departamento', None),
            "grupo": args.get('grupo', None),
            "instituto": args.get('instituto', None),
        }

        año_inicio = args.get('inicio', datetime.now().year)
        año_fin = args.get('fin', datetime.now().year)

        fuentes = {key: value for key,
                   value in fuentes.items() if value is not None}

        timestamp = datetime.now().strftime("%d%m%Y_%H%M")

        download_filename = f"informe_{list(fuentes.keys())[0]}_{list(fuentes.values())[0]}_{timestamp}.{tipo}"
        internal_filename = f"temp/{download_filename}"
        internal_filename_with_format = f"{internal_filename}.{tipo}"

        generar_informe(fuentes, año_inicio, año_fin, tipo, internal_filename)

        response = send_file(
            internal_filename_with_format, as_attachment=True,  download_name=download_filename)

        os.remove(internal_filename_with_format)

        return response
