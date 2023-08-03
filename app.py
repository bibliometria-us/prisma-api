# Import from the new location
from routes.investigador import investigador_namespace
from flask import Flask
from flask_restx import Api
import logging

prisma_base_url = "https://bibliometria.us.es/prisma"

app = Flask(__name__)
api = Api(app, version="1.0", title="Prisma API")
logging.basicConfig(level=logging.INFO)

api.add_namespace(investigador_namespace)

# ERRORES GLOBALES


@api.errorhandler(ValueError)  # Custom error handler for ValueError
def handle_invalid_accept_header(error):
    """Error handler for Invalid Accept header."""
    return {'message': 'Formato de salida no soportado'}, 406


if __name__ == '__main__':
    app.run()
