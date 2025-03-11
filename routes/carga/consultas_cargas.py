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


# ****************************************
# ************ QUALITY RULES *************
# ****************************************
# Regla de calidad p_00
# No es una regla de calidad - Obtiene la lista de las bibliotecas
def get_quality_rule_p_00(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT ib.nombre AS BIBLIOTECA FROM i_biblioteca ib;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad p_01
# Publicación con tipo de Datos duplicado
def get_quality_rule_p_01(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pdp.idPublicacion AS ID_PUBLICACION, GROUP_CONCAT(pdp.tipo, ": ", pdp.valor SEPARATOR ", ") AS DATO, ib.nombre AS BIBLIOTECA
                        FROM p_dato_publicacion pdp
                        LEFT JOIN (SELECT idPublicacion, MAX(idCentro) as idCentro, eliminado FROM publicacionesXcentro GROUP BY idPublicacion) p ON p.idPublicacion = pdp.idPublicacion
                        LEFT JOIN i_centro ic ON ic.idCentro = p.idCentro 
                        LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca 
                        WHERE p.eliminado = 0 AND pdp.tipo != "titulo_alt"
                        GROUP BY pdp.idPublicacion, pdp.tipo
                        HAVING COUNT(pdp.idPublicacion)>1;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad p_02
# Publicación con tipo de Identificadores duplicado
def get_quality_rule_p_02(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pi.idPublicacion AS ID_PUBLICACION, GROUP_CONCAT(pi.tipo, ": ", pi.valor SEPARATOR ", ") AS IDENTIFICADOR, ib.nombre AS BIBLIOTECA
                        FROM `p_identificador_publicacion` pi
                        LEFT JOIN (SELECT idPublicacion, MAX(idCentro) as idCentro, eliminado FROM publicacionesXcentro GROUP BY idPublicacion) p ON p.idPublicacion = pi.idPublicacion
                        LEFT JOIN i_centro ic ON ic.idCentro = p.idCentro 
                        LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca 
                        WHERE pi.eliminado = 0 AND p.eliminado = 0 AND pi.tipo != "doi"
                        GROUP BY pi.idPublicacion,pi.tipo
                        HAVING COUNT(pi.idPublicacion)>1;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad p_03
# Autores duplicados en publicación con mismo rol
def get_quality_rule_p_03(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pa.idPublicacion AS ID_PUBLICACION, GROUP_CONCAT( pa.rol ,": " , pa.firma SEPARATOR "; ") AS AUTOR, ib.nombre AS BIBLIOTECA
                        FROM p_autor pa
                        LEFT JOIN (SELECT pxc.idPublicacion, MAX(pxc.idCentro) as idCentro, pxc.eliminado FROM publicacionesXcentro pxc
                        INNER JOIN p_autor pa ON pa.idPublicacion = pxc.idPublicacion 
                        GROUP BY pxc.idPublicacion
                        HAVING COUNT(pa.idAutor) < 25) p ON p.idPublicacion = pa.idPublicacion
                        LEFT JOIN i_centro ic ON ic.idCentro = p.idCentro 
                        LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca 
                        WHERE p.eliminado = 0
                        GROUP BY pa.idPublicacion, pa.firma, pa.rol
                        HAVING COUNT(pa.idAutor) >1;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad p_04
# Publicación sin autores US
def get_quality_rule_p_04(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pa.idPublicacion AS ID_PUBLICACION FROM p_publicacion pp 
                            INNER JOIN p_autor pa ON pa.idPublicacion = pp.idPublicacion 
                            WHERE pp.eliminado = 0 AND pp.tipo != "Tesis"
                            GROUP BY pp.idPublicacion
                            HAVING COUNT(CASE WHEN pa.idInvestigador = 0 THEN NULL ELSE pa.idInvestigador END) = 0;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica
