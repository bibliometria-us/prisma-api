from db.conexion import BaseDatos
from flask import request


def buscar_api_key(api_key: str) -> bool:
    db = BaseDatos(database="api")
    query = "SELECT COUNT(*) FROM api_key WHERE api_key = %s"
    params = [api_key]
    result = int(db.ejecutarConsulta(query, params)[1][0])

    if result == 1:
        return True

    return False


def comprobar_api_key(api_key: str, namespace):
    if not (request.referrer and request.referrer.startswith(request.host_url)):
        if not api_key or not buscar_api_key(api_key):
            namespace.abort(401, 'API Key inválida')
