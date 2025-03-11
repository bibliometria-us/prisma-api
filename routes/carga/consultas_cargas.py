from db.conexion import BaseDatos
from models.investigador import Investigador


def get_investigadores_activos(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT * FROM prisma.i_investigador_activo;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe().set_index("idInvestigador").to_dict(orient="index")
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_id_investigadores(id_investigador, bd: BaseDatos = None) -> dict:
    params_publicacion = {"idInvestigador": id_investigador}

    query_publicacion = """SELECT idIdentificador, idInvestigador, tipo, valor FROM i_identificador_investigador WHERE idInvestigador = %(idInvestigador)s;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion, params_publicacion)
        result = bd.get_dataframe().set_index("idIdentificador").to_dict(orient="index")
    except Exception as e:
        return {"error": e.message}, 400
    return result


def get_n_first_dois(n_dois, bd: BaseDatos = None) -> list[str]:
    params_publicacion = {"limit": n_dois}

    query_publicacion = """SELECT pip.valor FROM prisma.p_identificador_publicacion pip WHERE tipo = "doi" LIMIT %(limit)s;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion, params_publicacion)
        result = list(
            bd.get_dataframe().set_index("valor").to_dict(orient="index").keys()
        )
    except Exception as e:
        return {"error": e.message}, 400
    return result
