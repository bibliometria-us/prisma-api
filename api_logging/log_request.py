from db.conexion import BaseDatos
import fnmatch

route_blacklist = [
    "/",
    "/swagger*",
    "/auth*",
    "/favicon.ico"
    "/usuario*"

]


def log_request(route, args, response_code, user):
    # Cortar hasta la longitud máxima de las columnas de la base de datos
    route = route[0:1000]
    args = str(args)[0:1000]
    user = user[0:100]

    if is_in_blacklist(route, route_blacklist):
        return None

    db = BaseDatos(database="api")
    query = "INSERT INTO logs (route, args, response_code, user) VALUES (%s, %s, %s, %s)"
    params = [route, args, response_code, user]

    db.ejecutarConsulta(query, params)


def is_in_blacklist(s, blacklist):
    result = False
    for pattern in blacklist:
        if fnmatch.fnmatch(s, pattern):
            result = True
            break

    return result
