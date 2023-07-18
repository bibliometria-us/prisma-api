from db.conexion import BaseDatos


def comprobar_api_key(api_key: str) -> bool:
    db = BaseDatos(database="api")
    query = "SELECT COUNT(*) FROM api_key WHERE api_key = %s"
    params = [api_key]
    result = int(db.ejecutarConsulta(query, params)[1][0])

    if result == 1:
        return True

    return False
