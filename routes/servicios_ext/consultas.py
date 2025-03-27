from db.conexion import BaseDatos
from models.investigador import Investigador


# ****************************************
# *************   BASICO   ***************
# ****************************************
# Obtiene la lista de las bibliotecas
def get_bibliotecas(bd: BaseDatos = None) -> dict:
    query = """SELECT ib.idBiblioteca, ib.nombre AS BIBLIOTECA FROM i_biblioteca ib;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# Obtiene la lista de los centros
def get_centros(bd: BaseDatos = None) -> dict:
    query = """SELECT ic.idCentro, ic.nombre AS CENTRO FROM i_centro ic;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# Obtiene la lista de los departamentos
def get_departamentos(bd: BaseDatos = None) -> dict:
    query = """SELECT id.idDepartamento, id.nombre AS DEPARTAMENTO FROM i_departamento id;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# Obtiene la lista de las areas
def get_areas(bd: BaseDatos = None) -> dict:
    query = """SELECT ia.idArea, ia.nombre AS AREA FROM i_area ia;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# Obtiene la lista de los institutos
def get_institutos(bd: BaseDatos = None) -> dict:
    query = """SELECT ii.idInstituto , ii.nombre AS INSTITUTO FROM i_instituto ii;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# Obtiene la lista de centros de excelencia
def get_centros_excelencia(bd: BaseDatos = None) -> dict:
    query = """SELECT iue.idUdExcelencia, iue.nombre AS UD_EXELENCIA FROM i_unidad_excelencia iue;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# ****************************************
# ********** INF. BIBLIOMETRICO **********
# ****************************************
# ****************************************
def get_investigadores(bd: BaseDatos = None) -> dict:
    query = """SELECT iia.idInvestigador AS Id_Prisma,
                        iia.nombre AS Nombre,
                        iia.apellidos AS Apellidos,
                        id.idDepartamento AS Id_Departamento,
                        id.nombre AS Departamento,
                        ia.idArea AS Id_Area,
                        ia.nombre AS Area, 
                        ig.idGrupo AS Id_Grupo,
                        ig.nombre AS Grupo,
                        ic.idCentro AS Id_Centro,
                        ic.nombre AS Centro,
                        ii.idInstituto AS Id_Instituto,
                        ii.nombre AS Instituto,
                        iue.idUdExcelencia AS Id_UdExcelencia,
                        iue.nombre AS Ud_Exelencia,
                        MAX(CASE WHEN iii.tipo = 'scopus' THEN iii.valor END) AS Id_Scopus,
                        MAX(CASE WHEN iii.tipo = 'researcherId' THEN iii.valor END) AS Id_Wos,
                        MAX(CASE WHEN iii.tipo = 'openalex' THEN iii.valor END) AS Id_Openalex 
                        FROM i_investigador_activo iia
                        LEFT JOIN i_departamento id ON id.idDepartamento = iia.idDepartamento
                        LEFT JOIN i_area ia ON ia.idArea = iia.idArea 
                        LEFT JOIN i_grupo_investigador igi ON igi.idInvestigador = iia.idInvestigador
                        LEFT JOIN i_grupo ig ON ig.idGrupo = igi.idGrupo
                        LEFT JOIN i_centro ic ON ic.idCentro = iia.idCentro
                        LEFT JOIN i_miembro_instituto imi ON imi.idInvestigador = iia.idInvestigador
                        LEFT JOIN i_instituto ii ON ii.idInstituto = imi.idInstituto
                        LEFT JOIN i_miembro_unidad_excelencia imue ON imue.idInvestigador = iia.idInvestigador
                        LEFT JOIN i_unidad_excelencia iue ON iue.idUdExcelencia = imue.idUdExcelencia
                        LEFT JOIN i_identificador_investigador iii ON iii.idInvestigador = iia.idInvestigador
                        GROUP BY iia.idInvestigador ORDER BY Apellidos, Nombre;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# ****************************************
# ************ QUALITY RULES *************
# ****************************************
# Regla de calidad p_00
# Últimas 100 publicaciones introducidas
def get_quality_rule_p_00(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pxc.idPublicacion AS ID_PUBLICACION,
                            ib.idBiblioteca AS idBiblioteca,
                            ib.nombre AS BIBLIOTECA,
                            CONCAT(pf.agno,"-", pf.mes) AS FECHA_INCLUSION
                            FROM publicacionesXcentro pxc
                            LEFT JOIN i_centro ic ON ic.idCentro = pxc.idCentro
                            LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                            LEFT JOIN p_fecha_publicacion pf ON pf.idPublicacion = pxc.idPublicacion
                            WHERE pxc.eliminado = 0 AND pf.tipo = "creacion"
                            GROUP BY pxc.idPublicacion
                            HAVING COUNT(DISTINCT pxc.idCentro) > 1
                            ORDER BY pf.agno, pf.mes
                            DESC LIMIT 100;"""
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
    query_publicacion = """SELECT pdp.idPublicacion AS ID_PUBLICACION, GROUP_CONCAT(pdp.tipo, ": ", pdp.valor SEPARATOR ", ") AS DATO, ib.idBiblioteca AS idBiblioteca,  ib.nombre AS BIBLIOTECA
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


# # Regla de calidad p_02
# # Publicación con tipo de Identificadores duplicado
# def get_quality_rule_p_02(bd: BaseDatos = None) -> dict:
#     query_publicacion = """SELECT pi.idPublicacion AS ID_PUBLICACION, GROUP_CONCAT(pi.tipo, ": ", pi.valor SEPARATOR ", ") AS IDENTIFICADOR, ib.nombre AS BIBLIOTECA
#                         FROM `p_identificador_publicacion` pi
#                         LEFT JOIN (SELECT idPublicacion, MAX(idCentro) as idCentro, eliminado FROM publicacionesXcentro GROUP BY idPublicacion) p ON p.idPublicacion = pi.idPublicacion
#                         LEFT JOIN i_centro ic ON ic.idCentro = p.idCentro
#                         LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
#                         WHERE pi.eliminado = 0 AND p.eliminado = 0 AND pi.tipo != "doi"
#                         GROUP BY pi.idPublicacion,pi.tipo
#                         HAVING COUNT(pi.idPublicacion)>1;"""
#     try:
#         if bd is None:
#             bd = BaseDatos()
#         bd.ejecutarConsulta(query_publicacion)
#         metrica = bd.get_dataframe()
#     except Exception as e:
#         return {"error": e.message}, 400
#     return metrica


# # Regla de calidad p_03
# # Autores duplicados en publicación con mismo rol
# def get_quality_rule_p_03(bd: BaseDatos = None) -> dict:
#     query_publicacion = """SELECT pa.idPublicacion AS ID_PUBLICACION, GROUP_CONCAT( pa.rol ,": " , pa.firma SEPARATOR "; ") AS AUTOR, ib.nombre AS BIBLIOTECA
#                         FROM p_autor pa
#                         LEFT JOIN (SELECT pxc.idPublicacion, MAX(pxc.idCentro) as idCentro, pxc.eliminado FROM publicacionesXcentro pxc
#                         INNER JOIN p_autor pa ON pa.idPublicacion = pxc.idPublicacion
#                         GROUP BY pxc.idPublicacion
#                         HAVING COUNT(pa.idAutor) < 25) p ON p.idPublicacion = pa.idPublicacion
#                         LEFT JOIN i_centro ic ON ic.idCentro = p.idCentro
#                         LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
#                         WHERE p.eliminado = 0
#                         GROUP BY pa.idPublicacion, pa.firma, pa.rol
#                         HAVING COUNT(pa.idAutor) >1;"""
#     try:
#         if bd is None:
#             bd = BaseDatos()
#         bd.ejecutarConsulta(query_publicacion)
#         metrica = bd.get_dataframe()
#     except Exception as e:
#         return {"error": e.message}, 400
#     return metrica


# # Regla de calidad p_04
# # Publicación sin autores US
# def get_quality_rule_p_04(bd: BaseDatos = None) -> dict:
#     query_publicacion = """SELECT pa.idPublicacion AS ID_PUBLICACION FROM p_publicacion pp
#                             INNER JOIN p_autor pa ON pa.idPublicacion = pp.idPublicacion
#                             WHERE pp.eliminado = 0 AND pp.tipo != "Tesis"
#                             GROUP BY pp.idPublicacion
#                             HAVING COUNT(CASE WHEN pa.idInvestigador = 0 THEN NULL ELSE pa.idInvestigador END) = 0;"""
#     try:
#         if bd is None:
#             bd = BaseDatos()
#         bd.ejecutarConsulta(query_publicacion)
#         metrica = bd.get_dataframe()
#     except Exception as e:
#         return {"error": e.message}, 400
#     return metrica
