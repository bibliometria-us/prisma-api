from db.conexion import BaseDatos


def obtener_tipos_publicacion():
    bd = BaseDatos(database="config")

    query = "SELECT nombre FROM tipos_publicacion WHERE activo = true"
    resultado = bd.ejecutarConsulta(query)

    tipos_publicacion = [row[0] for row in resultado[1:]]
    return tipos_publicacion


def mapear_tipo_publicacion(origen: str, nombre_origen: str):
    bd = BaseDatos(database="config")

    query = """SELECT nombre_prisma FROM tipos_publicacion_map tpm
            LEFT JOIN tipos_publicacion tp ON tp.nombre = tpm.nombre_prisma
            WHERE tpm.nombre_origen = %(nombre_origen)s AND tpm.origen = %(origen)s AND tp.activo = true"""
    params = {"nombre_origen": nombre_origen, "origen": origen}

    bd.ejecutarConsulta(query, params)
    result = bd.get_first_cell()

    return result
