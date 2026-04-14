from db.conexion import BaseDatos


def obtener_tipos_fuente():
    bd = BaseDatos(database="config")

    query = "SELECT nombre FROM tipos_fuente WHERE activo = true"
    resultado = bd.ejecutarConsulta(query)

    tipos_fuente = [row[0] for row in resultado[1:]]
    return tipos_fuente


def mapear_tipo_fuente(origen: str, nombre_origen: str):
    bd = BaseDatos(database="config")

    query = """SELECT nombre_prisma FROM tipos_fuente_map tfm
            LEFT JOIN tipos_fuente tf ON tf.nombre = tfm.nombre_prisma
            WHERE tfm.nombre_origen = %(nombre_origen)s AND tfm.origen = %(origen)s AND tf.activo = true"""
    params = {"nombre_origen": nombre_origen, "origen": origen}

    bd.ejecutarConsulta(query, params)
    result = bd.get_first_cell()

    return result
