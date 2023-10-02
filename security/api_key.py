from db.conexion import BaseDatos
from flask import request
import string
import secrets


def buscar_api_key(api_key: str) -> bool:
    db = BaseDatos(database="api")
    query = "SELECT COUNT(*) FROM api_key WHERE apikey = %s"
    params = [api_key]
    result = int(db.ejecutarConsulta(query, params)[1][0])

    if result == 1:
        return True

    return False


def comprobar_api_key(api_key: str, namespace):
    if not (request.referrer and request.referrer.startswith(request.host_url)):
        if not api_key or not buscar_api_key(api_key):
            namespace.abort(401, 'API Key inválida')


def api_key_from_user(mail: str):

    db = BaseDatos(database="api")
    user = mail.split("@")[0]
    query = "SELECT apikey FROM api_key WHERE uvus = %s"
    params = [user]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result


def create_api_key(mail: str, length=40):

    db = BaseDatos(database="api")
    user = mail.split("@")[0]
    query = """
            INSERT INTO api_key (uvus, apikey)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE apikey = %s;
            """
    characters = string.ascii_lowercase + string.digits
    api_key = ''.join(secrets.choice(characters) for _ in range(length))

    params = [user, api_key, api_key]

    db.ejecutarConsulta(query, params)
