from db.conexion import BaseDatos
from models.investigador import Investigador


# ****************************************
# ******** FUNCIONES TEMPORALES **********
# ****************************************
# Obtiene la lista de las bibliotecas
def eliminar_autores_pub(id_publicacion: int, bd: BaseDatos = None) -> dict:
    query = "DELETE FROM p_autor WHERE idPublicacion = %s"
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query, (id_publicacion,))
        return {"message": "Autores eliminados correctamente"}
    except Exception as e:
        return {"error": str(e)}, 400


# ****************************************
# *************   BASICO   ***************
# ****************************************
# Obtiene la lista de los centros
def get_centros(bd: BaseDatos = None) -> dict:
    query = """SELECT ic.idCentro, ib.nombre  AS CENTRO FROM i_centro ic;"""
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
                        iia.sexo AS Sexo, 
                        id.idDepartamento AS Id_Departamento,
                        id.nombre AS Departamento,
                        ia.idArea AS Id_Area,
                        ia.nombre AS Area, 
                        ig.idGrupo AS Id_Grupo,
                        ig.nombre AS Grupo,
                        ic.idCentro AS Id_Centro,
                        ib.nombre  AS Centro,
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
# ************** PUBLICACIONES ****************
# Regla de calidad p_00
# Últimas 100 publicaciones introducidas
def get_quality_rule_p_00(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pxc.idPublicacion AS ID_PUBLICACION, pxc.titulo AS TITULO, pxc.tipo AS TIPO,
(CASE WHEN pip.tipo = 'doi' THEN pip.valor END) AS DOI,
(CASE WHEN pip.tipo = 'scopus' THEN pip.valor END) AS ID_SCOPUS,
(CASE WHEN pip.tipo = 'wos' THEN pip.valor END) AS ID_WOS,
(CASE WHEN pip.tipo = 'openalex' THEN pip.valor END) AS ID_OPENALEX,
(CASE WHEN pip.tipo = 'idus' THEN pip.valor END) AS ID_IDUS,
(CASE WHEN pdp.tipo = 'volumen' THEN pdp.valor END) AS VOLUMEN,
(CASE WHEN pdp.tipo = 'numero' THEN pdp.valor END) AS NUMERO,
(CASE WHEN pdp.tipo = 'pag_inicio' THEN pdp.valor END) AS P_INICIO,
(CASE WHEN pdp.tipo = 'pag_fin' THEN pdp.valor END) AS P_FIN,
pf2.titulo AS FUENTE,
pf2.tipo AS TIPO_FUENTE,
(CASE WHEN pif.tipo = 'issn' THEN pif.valor END) AS ISSN,
(CASE WHEN pif.tipo = 'eissn' THEN pif.valor END) AS EISSN,
(CASE WHEN pif.tipo = 'isbn' THEN pif.valor END) AS ISBN,
(CASE WHEN pif.tipo = 'eisbn' THEN pif.valor END) AS EISBN,
ib.idBiblioteca AS idBiblioteca,
ib.nombre AS BIBLIOTECA,
CONCAT(pf.agno,"-", pf.mes) AS FECHA_INCLUSION
FROM publicacionesXcentro pxc
LEFT JOIN p_dato_publicacion pdp ON pdp.idPublicacion = pxc.idPublicacion
LEFT JOIN p_identificador_publicacion pip ON pip.idPublicacion = pxc.idPublicacion
LEFT JOIN p_autor pa ON pa.idPublicacion = pxc.idPublicacion
LEFT JOIN p_fuente pf2 ON pf2.idFuente = pxc.idFuente
LEFT JOIN p_dato_fuente pdf ON pdf.idFuente = pf2.idFuente
LEFT JOIN p_identificador_fuente pif  ON pif.idFuente = pf2.idFuente
LEFT JOIN i_centro ic ON ic.idCentro = pxc.idCentro
LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
LEFT JOIN p_fecha_publicacion pf ON pf.idPublicacion = pxc.idPublicacion
WHERE pxc.eliminado = 0 AND pf.tipo = "creacion"
GROUP BY pxc.idPublicacion
HAVING COUNT(DISTINCT pxc.idCentro) > 1
ORDER BY pf.agno, pf.mes
DESC LIMIT 500;"""
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


# Regla de calidad p_02
# Publicación con tipo de Identificadores duplicado
def get_quality_rule_p_02(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pi.idPublicacion AS ID_PUBLICACION, GROUP_CONCAT(pi.tipo, ": ", pi.valor SEPARATOR ", ") AS IDENTIFICADOR, ib.idBiblioteca AS idBiblioteca, ib.nombre AS BIBLIOTECA
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
    query_publicacion = """SELECT pa.idPublicacion AS ID_PUBLICACION, GROUP_CONCAT( pa.rol ,": " , pa.firma SEPARATOR "; ") AS AUTOR, ib.idBiblioteca AS idBiblioteca, ib.nombre AS BIBLIOTECA
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
    query_publicacion = """SELECT pp.titulo AS TITULO, pp.agno AS AGNO, pa.idPublicacion AS ID_PUBLICACION FROM p_publicacion pp
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


# Regla de calidad p_05
# Publicaciones sin identificadores
def get_quality_rule_p_05(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT p.idPublicacion AS ID_PUBLICACION, p.titulo AS TITULO, ib.nombre AS BIBLIOTECA, p.agno AS AGNO
                    FROM (SELECT idPublicacion,titulo, MAX(idCentro) as idCentro, eliminado, agno  FROM publicacionesXcentro GROUP BY idPublicacion) p
                    LEFT JOIN (SELECT * FROM `p_identificador_publicacion` WHERE eliminado = 0) pi ON pi.idPublicacion = p.idPublicacion 
                    LEFT JOIN i_centro ic ON ic.idCentro = p.idCentro 
                    LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca 
                    WHERE p.eliminado = 0 AND pi.idIdentificador IS NULL
                    GROUP BY p.idPublicacion;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad p_06
# Publicación duplicada por título y año
def get_quality_rule_p_06(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pf.titulo AS TITULO, GROUP_CONCAT("Id: ",pf.idPublicacion  SEPARATOR ", ")  AS IDS, GROUP_CONCAT("Título: ",pf.titulo  SEPARATOR ", ")  AS TITULOS, COUNT(pf.idPublicacion) AS N_PUBS
                        FROM p_publicacion pf 
                        WHERE pf.eliminado = 0 AND pf.titulo NOT IN ("Preface", "Introducción", "Presentación", "Prólogo", "Editorial", "Foreword", "Discussion", "Introduction", "Prefacio")
                        GROUP BY LOWER(TRIM(pf.titulo)), pf.agno
                        HAVING COUNT(pf.idPublicacion )>1 ORDER BY N_PUBS DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad p_07
# Publicación duplicada por título, año, tipo y fuente
def get_quality_rule_p_06(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pf.titulo AS TITULO, GROUP_CONCAT("Id: ",pf.idPublicacion  SEPARATOR ", ")  AS IDS, GROUP_CONCAT("Título: ",pf.titulo  SEPARATOR ", ")  AS TITULOS, COUNT(pf.idPublicacion) AS N_PUBS
                        FROM p_publicacion pf 
                        WHERE pf.eliminado = 0 AND pf.titulo NOT IN ("Preface", "Introducción", "Presentación", "Prólogo", "Editorial", "Foreword", "Discussion", "Introduction", "Prefacio")
                        GROUP BY LOWER(TRIM(pf.titulo)), pf.agno, pf.tipo, pf.idFuente
                        HAVING COUNT(pf.idPublicacion )>1 ORDER BY N_PUBS DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad p_08
# Publicación tipo capitulo cuya fuente sea tipo colección
def get_quality_rule_p_08(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT p.idPublicacion AS ID_PUBLICACION, p.titulo AS TITULO, ib.nombre AS BIBLIOTECA
        FROM (SELECT idPublicacion, titulo, MAX(idCentro) as idCentro, idFuente, eliminado FROM publicacionesXcentro  WHERE tipo = 'Capitulo' AND eliminado = 0  GROUP BY idPublicacion) p
        LEFT JOIN i_centro ic ON ic.idCentro = p.idCentro 
        LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca 
        LEFT JOIN (SELECT * FROM p_fuente WHERE tipo = "coleccion") pf ON pf.idFuente = p.idFuente
        WHERE pf.idFuente IS NOT NULL
        GROUP BY p.idPublicacion;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad p_15
# Últimas 100 publicaciones insertadas
def get_quality_rule_p_15(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT * FROM p_publicacion WHERE eliminado = 0 ORDER BY idPublicacion DESC LIMIT 200;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad p_16
# Publicación duplicada por título, año, tipo y fuente
def get_quality_rule_p_16(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pf.titulo AS TITULO, GROUP_CONCAT("Id: ",pf.idPublicacion  SEPARATOR ", ") AS IDS, pf2.titulo AS TITULO_FUENTE, COUNT(pf.idPublicacion) AS N_PUBS
        FROM p_publicacion pf 
        INNER JOIN p_fuente pf2 ON pf2.idFuente = pf.idFuente
        WHERE pf.eliminado = 0 AND pf.titulo NOT IN ("Preface", "Introducción", "Presentación", "Prólogo", "Editorial", "Foreword", "Discussion", "Introduction", "Prefacio") AND pf.tipo != "Tesis"
        GROUP BY LOWER(TRIM(pf.titulo)), pf.agno, LOWER(TRIM(pf2.titulo))
        HAVING COUNT(pf.idPublicacion )>1 ORDER BY N_PUBS DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# ************** FUENTES ****************
# Regla de calidad f_01
# Fuentes eliminadas con publicaciones asociadas
def get_quality_rule_f_01(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL, GROUP_CONCAT("Id_Pub: ", pp.idPublicacion SEPARATOR ", ") AS PUBLICACIONES, COUNT(pp.idPublicacion) AS N_PUBLICACIONES
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_publicacion` WHERE eliminado = 0) as pp ON pp.idFuente = pf.idFuente
            WHERE pf.eliminado = 1
            GROUP BY pp.idFuente
            HAVING COUNT(pp.idPublicacion) > 0;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_02
# Fuentes tipo libro sin ISBN o eISBN
def get_quality_rule_f_02(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_identificador_fuente` WHERE eliminado = 0 AND tipo IN ("isbn", "eisbn")) pif ON pif.idFuente  = pf.idFuente 
            WHERE pf.eliminado = 0 AND (pf.tipo IN ("Libro", "Books")) AND pif.idIdentificador IS NULL
            GROUP BY pf.idFuente 
            HAVING COUNT(pif.idFuente) = 0;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_03
# Fuentes tipo libro sin ISBN o eISBN
def get_quality_rule_f_03(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_identificador_fuente` WHERE eliminado = 0 AND tipo = "isbn" ) pif ON pif.idFuente  = pf.idFuente 
            WHERE pf.eliminado = 0 AND (pf.tipo IN ("Libro","Books")) AND pif.idIdentificador IS NOT NULL
            GROUP BY pf.idFuente 
            HAVING COUNT(pif.idFuente) > 1 ;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_04
# Fuentes tipo libro con mas de 1 eISBN
def get_quality_rule_f_04(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_identificador_fuente` WHERE eliminado = 0 AND tipo = "eisbn" ) pif ON pif.idFuente  = pf.idFuente 
            WHERE pf.eliminado = 0 AND (pf.tipo IN ("Libro","Books")) AND pif.idIdentificador IS NOT NULL
            GROUP BY pf.idFuente 
            HAVING COUNT(pif.idFuente) > 1 ;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_05
# Fuentes tipo revista sin ISSN o eISBN
def get_quality_rule_f_05(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_identificador_fuente` WHERE eliminado = 0 AND tipo IN ("issn", "eissn")) pif ON pif.idFuente  = pf.idFuente 
            WHERE pf.eliminado = 0 AND (pf.tipo = "Revista") AND pif.idIdentificador IS NULL
            GROUP BY pf.idFuente 
            HAVING COUNT(pif.idFuente) = 0;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_06
# Fuentes tipo revista con mas de 1 ISSN
def get_quality_rule_f_06(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_identificador_fuente` WHERE eliminado = 0 AND tipo = "issn" ) pif ON pif.idFuente  = pf.idFuente 
            WHERE pf.eliminado = 0 AND (pf.tipo = "Revista") AND pif.idIdentificador IS NOT NULL
            GROUP BY pf.idFuente 
            HAVING COUNT(pif.idFuente) > 1 ;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_07
# Fuentes tipo revista con mas de 1 eISSN
def get_quality_rule_f_07(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_identificador_fuente` WHERE eliminado = 0 AND tipo = "eissn" ) pif ON pif.idFuente  = pf.idFuente 
            WHERE pf.eliminado = 0 AND (pf.tipo = "Revista") AND pif.idIdentificador IS NOT NULL
            GROUP BY pf.idFuente 
            HAVING COUNT(pif.idFuente) > 1 ;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_08
# Fuentes con ISSN/eISSN con formato incorrecto
def get_quality_rule_f_08(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL, GROUP_CONCAT("ISSN: ",pif.valor  SEPARATOR ", ") AS PUBLICACIONES
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_identificador_fuente` WHERE eliminado = 0 AND tipo IN ("issn", "eissn") AND valor NOT REGEXP '^[0-9]{4}-[0-9]{3}[0-9X]$') pif ON pif.idFuente  = pf.idFuente 
            WHERE pf.eliminado = 0 AND pif.idIdentificador IS NOT NULL
            GROUP BY pf.idFuente;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_09
# Fuentes con ISBN/eISBN con formato incorrecto
def get_quality_rule_f_09(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL, GROUP_CONCAT("ISBN: ",pif.valor  SEPARATOR ", ") AS IDENTIFICADORES
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_identificador_fuente` 
            WHERE eliminado = 0 AND tipo IN ("isbn", "eisbn") AND valor NOT REGEXP '^[0-9][0-9\\-]{8,16}[0-9Xx]$' AND
            NOT(CHAR_LENGTH(REPLACE(valor, '-', '')) = 10 OR CHAR_LENGTH(REPLACE(valor, '-', '')) = 13)) pif ON pif.idFuente  = pf.idFuente 
            WHERE pf.eliminado = 0 AND pif.idIdentificador IS NOT NULL
            GROUP BY pf.idFuente;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_10
# Fuente tipo colección sin ISSN
def get_quality_rule_f_10(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.idFuente AS idFuente, pf.titulo AS TITULO, pf.editorial AS EDITORIAL
            FROM p_fuente pf 
            LEFT JOIN (SELECT * FROM `p_identificador_fuente` 
            WHERE eliminado = 0 AND tipo IN ("issn", "eissn")) pif ON pif.idFuente  = pf.idFuente 
            WHERE pf.eliminado = 0 AND pf.tipo = "colección" AND pif.idIdentificador IS NULL
            GROUP BY pf.idFuente;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_11
# Títulos de fuentes duplicados
def get_quality_rule_f_11(bd: BaseDatos = None) -> dict:
    query = """SELECT pf.titulo AS TITULO, GROUP_CONCAT("Id: ",pf.idFuente  SEPARATOR ", ")  AS IDS, GROUP_CONCAT("Título: ",pf.titulo  SEPARATOR ", ")  AS TITULOS, COUNT(pf.idFuente) AS N_FUENTES
            FROM p_fuente pf 
            WHERE pf.eliminado = 0
            GROUP BY LOWER(TRIM(pf.titulo))
            HAVING COUNT(pf.idFuente)>1;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Regla de calidad f_16
# Títulos de fuentes duplicados
def get_quality_rule_f_16(bd: BaseDatos = None) -> dict:
    query = """SELECT idFuente AS ID, tipo AS TIPO, origen AS ORIGEN FROM prisma.p_fuente AS pf
            WHERE titulo= "" AND eliminado ="0";"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# ****************************************
# ************ INVESTIGADORES *************
# ****************************************


# Regla de calidad i_02
# Últimos 50 investigadores
def get_quality_rule_i_02(bd: BaseDatos = None) -> dict:
    query = """SELECT iia.nombre AS Nombre, iia.apellidos AS Apellidos,
            CASE WHEN iia.sexo = 1 THEN 'Hombre'
            WHEN iia.sexo = 0 THEN 'Mujer'
            ELSE 'Desconocido'END AS Genero,
            id.nombre AS Departamento,
            ig.nombre AS Grupo,
            ib.nombre  AS Centro,
            ii.nombre AS Instituto,
            iue.nombre AS Ud_Exelencia,
            iia.idInvestigador AS Id_Prisma,
            MAX(CASE WHEN iii.tipo = 'scopus' THEN iii.valor END) AS Id_Scopus,
            MAX(CASE WHEN iii.tipo = 'researcherId' THEN iii.valor END) AS Id_Wos,
            MAX(CASE WHEN iii.tipo = 'openalex' THEN iii.valor END) AS Id_Openalex
            FROM i_investigador_activo iia
            LEFT JOIN i_departamento id ON id.idDepartamento = iia.idDepartamento
            LEFT JOIN i_grupo_investigador igi ON igi.idInvestigador = iia.idInvestigador
            LEFT JOIN i_grupo ig ON ig.idGrupo = igi.idGrupo
            LEFT JOIN i_centro ic ON ic.idCentro = iia.idCentro
            LEFT JOIN i_miembro_instituto imi ON imi.idInvestigador = iia.idInvestigador
            LEFT JOIN i_instituto ii ON ii.idInstituto = imi.idInstituto
            LEFT JOIN i_miembro_unidad_excelencia imue ON imue.idInvestigador = iia.idInvestigador
            LEFT JOIN i_unidad_excelencia iue ON iue.idUdExcelencia = imue.idUdExcelencia
            LEFT JOIN i_identificador_investigador iii ON iii.idInvestigador = iia.idInvestigador
            LEFT JOIN (SELECT * FROM m_informes WHERE ambito = "investigador" ) mi ON mi.identificadorInt = iia.idInvestigador
            GROUP BY iia.idInvestigador
            ORDER BY iia.idInvestigador DESC LIMIT 50"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# ****************************************
# ************ PUBLICACIONES *************
# ****************************************
# *** BASICO ***
# Obtiene la lista de las bibliotecas
def get_bibliotecas(bd: BaseDatos = None) -> dict:
    query = """SELECT ib.idBiblioteca AS ID_BIBLIOTECA, ib.nombre AS BIBLIOTECA FROM i_biblioteca ib;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# Obtiene la lista de los tipo permitidos de publicaciones (incluidos en la tabla de configuración de tipos de publicación)
def get_tipos_publicaciones_permitidos(bd: BaseDatos = None) -> dict:
    query = (
        """SELECT tp.nombre FROM config.tipos_publicacion tp WHERE tp.activo = '1'"""
    )
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# *** Regla de validacion ***
# Lista de publicaciones con tipo no permitido (no incluido en la tabla de configuración de tipos de publicación)
def get_pub_publicaciones_con_tipos_no_permitidos(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pp.idPublicacion AS ID_PUB, pp.tipo AS TIPO, pp.titulo AS TITULO, 
pp.agno AS AGNO, ic.idBiblioteca AS ID_BIBLIOTECA , ib.nombre AS BIBLIOTECA
    FROM prisma.publicacionesXcentro pp
    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
    WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis' AND (pp.idFuente = '0' OR pp.idFuente IS NULL) AND pp.tipo NOT IN (SELECT nombre FROM config.tipos_publicacion WHERE activo = '1')
    ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones con tipo "Otros"
def get_pub_publicaciones_con_tipos_otros(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pp.idPublicacion AS ID_PUB, pp.tipo AS TIPO, pp.titulo AS TITULO, 
pp.agno AS AGNO, ic.idBiblioteca AS ID_BIBLIOTECA , ib.nombre  AS BIBLIOTECA
    FROM prisma.publicacionesXcentro pp
    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
    WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis' AND pp.tipo = "Otros"
    ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones con más de un ID del mismo tipo
def get_pub_publicaciones_mas_de_un_id_mismo_tipo(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                        pp.idPublicacion        AS ID_PUB,
                        pp.titulo               AS TITULO,
                        pp.tipo               AS TIPO,
                        pp.agno                 AS AGNO,
                        pip.tipo                AS TIPO_ID,
                        ic.idBiblioteca         AS ID_BIBLIOTECA,
                        ib.nombre               AS BIBLIOTECA
                    FROM prisma.publicacionesXcentro pp
                    LEFT JOIN p_identificador_publicacion pip ON pip.idPublicacion = pp.idPublicacion
                    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                    WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis'
                    AND (pp.idPublicacion, pip.tipo) IN (
                        SELECT idPublicacion, tipo
                        FROM p_identificador_publicacion
                        GROUP BY idPublicacion, tipo
                        HAVING COUNT(*) > 1
                    )
                    GROUP BY pp.idPublicacion, pp.titulo, pp.agno, pip.tipo, ic.idBiblioteca, ib.nombre 
                    ORDER BY pp.idPublicacion DESC, pip.tipo;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones sin ID
def get_pub_publicaciones_sin_id(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                        pp.idPublicacion        AS ID_PUB,
                        pp.tipo                 AS TIPO,
                        pp.titulo               AS TITULO,
                        pp.agno                 AS AGNO,
                        ic.idBiblioteca         AS ID_BIBLIOTECA,
                        ib.nombre               AS BIBLIOTECA
                    FROM prisma.publicacionesXcentro pp
                    LEFT JOIN p_identificador_publicacion pip ON pip.idPublicacion = pp.idPublicacion
                    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                    WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis'
                    AND pip.idPublicacion IS NULL
                    ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones con autores repetidos (misma firma) en la misma publicación
def get_pub_publicaciones_autores_repetidos(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                    pp.idPublicacion        AS ID_PUB,
                    pp.tipo                 AS TIPO,
                    pp.titulo               AS TITULO,
                    pp.agno                 AS AGNO,
                    ic.idBiblioteca         AS ID_BIBLIOTECA,
                    ib.nombre               AS BIBLIOTECA,
                    GROUP_CONCAT(DISTINCT firmas_dup.firma ORDER BY firmas_dup.firma SEPARATOR '; ') AS FIRMAS_DUPLICADAS
                FROM prisma.publicacionesXcentro pp
                LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                JOIN (
                    SELECT idPublicacion, firma
                    FROM p_autor
                    WHERE eliminado = '0'
                    GROUP BY idPublicacion, firma
                    HAVING COUNT(*) > 1
                ) firmas_dup ON firmas_dup.idPublicacion = pp.idPublicacion
                WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis'
                GROUP BY pp.idPublicacion, pp.tipo, pp.titulo, pp.agno, ic.idBiblioteca, ib.nombre 
                ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones sin fuente
def get_pub_publicaciones_sin_fuente(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pp.idPublicacion AS ID_PUB, pp.tipo AS TIPO, pp.titulo AS TITULO, 
                        pp.agno AS AGNO, ic.idBiblioteca AS ID_BIBLIOTECA , ib.nombre AS BIBLIOTECA
                        FROM prisma.publicacionesXcentro pp
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis' AND (pp.idFuente = '0' OR pp.idFuente IS NULL)
                        ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones sin autores US
def get_pub_publicaciones_sin_autores_us(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pp.idPublicacion AS ID_PUB, pp.tipo AS TIPO, pp.titulo AS TITULO,
                        pp.agno AS AGNO
                            FROM prisma.p_publicacion  pp
                            WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis'
                            AND pp.idPublicacion NOT IN (
                                SELECT DISTINCT idPublicacion
                                FROM prisma.p_autor
                                WHERE idInvestigador IS NOT NULL
                                AND eliminado = '0'
                            )
                            ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones con más de un Dato del mismo tipo
def get_pub_publicaciones_mas_de_un_dato_mismo_tipo(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                        pp.idPublicacion        AS ID_PUB,
                        pp.titulo               AS TITULO,
                        pp.tipo               AS TIPO,
                        pp.agno                 AS AGNO,
                        pip.tipo                AS TIPO_ID,
                        ic.idBiblioteca         AS ID_BIBLIOTECA,
                        ib.nombre               AS BIBLIOTECA
                    FROM prisma.publicacionesXcentro pp
                    LEFT JOIN p_dato_publicacion pip ON pip.idPublicacion = pp.idPublicacion
                    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                    WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis' 
                    AND (pp.idPublicacion, pip.tipo) IN (
                        SELECT idPublicacion, tipo
                        FROM p_dato_publicacion
                        GROUP BY idPublicacion, tipo
                        HAVING COUNT(*) > 1
                    )
                    GROUP BY pp.idPublicacion, pp.titulo, pp.agno, pip.tipo, ic.idBiblioteca, ib.nombre 
                    ORDER BY pp.idPublicacion DESC, pip.tipo;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


#  Lista de publicaciones repetidas Título, tipo, año.
def get_pub_publicaciones_repetidas_titulo_tipo_agno(bd: BaseDatos = None) -> dict:
    query_publicacion = """WITH publicaciones_normalizadas AS (
                        SELECT
                            idPublicacion,
                            tipo,
                            agno,
                            titulo,
                            idCentro,
                            LOWER(TRIM(REGEXP_REPLACE(titulo, '[^a-zA-Z0-9]', ''))) AS titulo_normalizado
                        FROM prisma.publicacionesXcentro
                        WHERE eliminado = '0'
                    ),
                    duplicados AS (
                        SELECT titulo_normalizado, tipo, agno
                        FROM publicaciones_normalizadas
                        GROUP BY titulo_normalizado, tipo, agno
                        HAVING COUNT(DISTINCT idPublicacion) > 1
                    )
                    SELECT
                        pn.tipo                 AS TIPO,
                        pn.agno                 AS AGNO,
                        MIN(pn.titulo)          AS TITULO,
                        ic.idBiblioteca         AS ID_BIBLIOTECA,
                        ib.nombre                AS BIBLIOTECA,
                        COUNT(DISTINCT pn.idPublicacion) AS NUM_PUBLICACIONES,
                        GROUP_CONCAT(DISTINCT pn.idPublicacion ORDER BY pn.idPublicacion SEPARATOR ', ') AS IDS_PUBLICACIONES
                    FROM publicaciones_normalizadas pn
                    JOIN duplicados d ON d.titulo_normalizado = pn.titulo_normalizado
                        AND d.tipo = pn.tipo
                        AND d.agno = pn.agno
                    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pn.idCentro
                    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                    GROUP BY pn.tipo, pn.agno, pn.titulo_normalizado, ic.idBiblioteca, ib.nombre 
                    ORDER BY pn.tipo, pn.agno;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones repetidas Título, tipo y Titulo de Fuente.
def get_pub_publicaciones_repetidas_titulo_tipo_fuente(bd: BaseDatos = None) -> dict:
    query_publicacion = """WITH publicaciones_normalizadas AS (
                        SELECT
                            pp.idPublicacion,
                            pp.tipo,
                            pp.titulo,
                            pp.agno,
                            pp.idCentro,
                            pf.titulo AS TITULO_FUENTE,
                            LOWER(TRIM(REGEXP_REPLACE(pp.titulo, '[^a-zA-Z0-9]', ''))) AS titulo_normalizado
                        FROM prisma.publicacionesXcentro pp
                        LEFT JOIN p_fuente pf ON pf.idFuente = pp.idFuente
                        WHERE pp.eliminado = '0'
                    ),
                    duplicados AS (
                        SELECT titulo_normalizado, tipo, TITULO_FUENTE
                        FROM publicaciones_normalizadas
                        GROUP BY titulo_normalizado, tipo, TITULO_FUENTE
                        HAVING COUNT(DISTINCT idPublicacion) > 1
                    )
                    SELECT
                        pn.tipo                 AS TIPO,
                        MIN(pn.titulo)          AS TITULO,
                        pn.TITULO_FUENTE        AS TITULO_FUENTE,
                        ic.idBiblioteca         AS ID_BIBLIOTECA,
                        ib.nombre                AS BIBLIOTECA,
                        COUNT(DISTINCT pn.idPublicacion) AS NUM_PUBLICACIONES,
                        GROUP_CONCAT(DISTINCT pn.idPublicacion ORDER BY pn.idPublicacion SEPARATOR ', ') AS IDS_PUBLICACIONES
                    FROM publicaciones_normalizadas pn
                    JOIN duplicados d ON d.titulo_normalizado = pn.titulo_normalizado
                        AND d.tipo = pn.tipo
                        AND d.TITULO_FUENTE = pn.TITULO_FUENTE
                    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pn.idCentro
                    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                    GROUP BY pn.tipo, pn.titulo_normalizado, pn.TITULO_FUENTE, ic.idBiblioteca, ib.nombre 
                    ORDER BY pn.tipo, pn.TITULO_FUENTE;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones tipo capitulo cuya fuente sea una colección.
def get_pub_publicaciones_tipo_capitulo_fuente_coleccion(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                            pp.idPublicacion        AS ID_PUB,
                            pp.tipo                 AS TIPO,
                            pp.titulo               AS TITULO,
                            pp.agno                 AS AGNO,
                            ic.idBiblioteca         AS ID_BIBLIOTECA,
                            ib.nombre                AS BIBLIOTECA,
                            pf.titulo               AS TITULO_FUENTE
                        FROM prisma.publicacionesXcentro pp
                        LEFT JOIN p_fuente pf ON pf.idFuente = pp.idFuente
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        WHERE pp.eliminado = '0'
                        AND pp.tipo = 'Capítulo'
                        AND pf.tipo = 'Colección'
                        ORDER BY ic.idBiblioteca, pp.agno DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones asociadas a fuentes eliminadas.
def get_pub_publicaciones_asociadas_a_fuentes_eliminadas(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                        pp.idPublicacion        AS ID_PUB,
                        pp.tipo                 AS TIPO,
                        pp.titulo               AS TITULO,
                        pp.agno                 AS AGNO,
                        ic.idBiblioteca         AS ID_BIBLIOTECA,
                        ib.nombre                AS BIBLIOTECA,
                        pf.titulo               AS TITULO_FUENTE
                    FROM prisma.publicacionesXcentro pp
                    LEFT JOIN p_fuente pf ON pf.idFuente = pp.idFuente
                    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                    WHERE pp.eliminado = '0'
                    AND pf.eliminado = '1'
                    ORDER BY ic.idBiblioteca, pp.agno DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de Últimas 200 publicaciones insertadas.
def get_pub_ultimas_200(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                            pp.idPublicacion        AS ID_PUB,
                            pp.tipo                 AS TIPO,
                            pp.titulo               AS TITULO,
                            pp.agno                 AS AGNO,
                            ic.idBiblioteca         AS ID_BIBLIOTECA,
                            ib.nombre                AS BIBLIOTECA,
                            pf.titulo               AS TITULO_FUENTE
                        FROM prisma.publicacionesXcentro pp
                        LEFT JOIN p_fuente pf ON pf.idFuente = pp.idFuente
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        WHERE pp.eliminado = '0'
                        ORDER BY pp.idPublicacion DESC
                        LIMIT 200;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones con idScopus y que la revista asociada no tenga metrica en SJR/Citiscore
def get_pub_publicaciones_scopus_sin_metrica_Revista(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                            pp.idPublicacion        AS ID_PUB,
                            pp.tipo                 AS TIPO,
                            pp.titulo               AS TITULO,
                            pp.agno                 AS AGNO,
                            ic.idBiblioteca         AS ID_BIBLIOTECA,
                            ib.nombre               AS BIBLIOTECA,
                            pf.titulo               AS TITULO_FUENTE
                        FROM prisma.publicacionesXcentro pp
                        LEFT JOIN p_fuente pf ON pf.idFuente = pp.idFuente
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        LEFT JOIN (
                            SELECT pif.idFuente
                            FROM p_identificador_fuente pif
                            INNER JOIN m_sjr sjr ON pif.valor = sjr.issn
                            WHERE pif.tipo IN ('issn', 'eissn')
                            UNION
                            SELECT pif.idFuente
                            FROM p_identificador_fuente pif
                            INNER JOIN m_sjr sjr ON pif.valor = sjr.issn_2
                            WHERE pif.tipo IN ('issn', 'eissn')
                            UNION
                            SELECT pif.idFuente
                            FROM p_identificador_fuente pif
                            INNER JOIN m_citescore cs ON pif.valor = cs.issn
                            WHERE pif.tipo IN ('issn', 'eissn')
                        ) fuentes_con_metrica ON fuentes_con_metrica.idFuente = pf.idFuente
                        WHERE pp.eliminado = '0'
                        AND pf.idFuente IS NOT NULL
                        AND pf.eliminado = '0'
                        AND pf.tipo = 'Revista'
                        AND EXISTS (
                            SELECT 1 FROM p_identificador_publicacion pip
                            WHERE pip.idPublicacion = pp.idPublicacion
                            AND pip.tipo = 'scopus'
                        )
                        AND fuentes_con_metrica.idFuente IS NULL
                        ORDER BY ic.idBiblioteca, pp.idPublicacion DESC;SELECT
                            pp.idPublicacion        AS ID_PUB,
                            pp.tipo                 AS TIPO,
                            pp.titulo               AS TITULO,
                            pp.agno                 AS AGNO,
                            ic.idBiblioteca         AS ID_BIBLIOTECA,
                            ib.nombre               AS BIBLIOTECA,
                            pf.titulo               AS TITULO_FUENTE
                        FROM prisma.publicacionesXcentro pp
                        LEFT JOIN p_fuente pf ON pf.idFuente = pp.idFuente
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        LEFT JOIN (
                            SELECT pif.idFuente
                            FROM p_identificador_fuente pif
                            INNER JOIN m_sjr sjr ON pif.valor = sjr.issn
                            WHERE pif.tipo IN ('issn', 'eissn')
                            UNION
                            SELECT pif.idFuente
                            FROM p_identificador_fuente pif
                            INNER JOIN m_sjr sjr ON pif.valor = sjr.issn_2
                            WHERE pif.tipo IN ('issn', 'eissn')
                            UNION
                            SELECT pif.idFuente
                            FROM p_identificador_fuente pif
                            INNER JOIN m_citescore cs ON pif.valor = cs.issn
                            WHERE pif.tipo IN ('issn', 'eissn')
                        ) fuentes_con_metrica ON fuentes_con_metrica.idFuente = pf.idFuente
                        WHERE pp.eliminado = '0'
                        AND pf.idFuente IS NOT NULL
                        AND pf.eliminado = '0'
                        AND pf.tipo = 'Revista'
                        AND EXISTS (
                            SELECT 1 FROM p_identificador_publicacion pip
                            WHERE pip.idPublicacion = pp.idPublicacion
                            AND pip.tipo = 'scopus'
                        )
                        AND fuentes_con_metrica.idFuente IS NULL
                        ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones con idWOS y que la revista asociada no tenga metrica en JCR ni JCI
def get_pub_publicaciones_wos_sin_metrica_Revista(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                        pp.idPublicacion        AS ID_PUB,
                        pp.tipo                 AS TIPO,
                        pp.titulo               AS TITULO,
                        pp.agno                 AS AGNO,
                        ic.idBiblioteca         AS ID_BIBLIOTECA,
                        ib.nombre               AS BIBLIOTECA,
                        pf.titulo               AS TITULO_FUENTE
                    FROM prisma.publicacionesXcentro pp
                    LEFT JOIN p_fuente pf ON pf.idFuente = pp.idFuente
                    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                    LEFT JOIN (
                        SELECT pif.idFuente
                        FROM p_identificador_fuente pif
                        INNER JOIN m_jcr jcr ON pif.valor = jcr.issn
                        WHERE pif.tipo IN ('issn', 'eissn')
                        UNION
                        SELECT pif.idFuente
                        FROM p_identificador_fuente pif
                        INNER JOIN m_jcr jcr ON pif.valor = jcr.issn_2
                        WHERE pif.tipo IN ('issn', 'eissn')
                        UNION
                        SELECT pif.idFuente
                        FROM p_identificador_fuente pif
                        INNER JOIN m_jci jci ON pif.valor = jci.issn
                        WHERE pif.tipo IN ('issn', 'eissn')
                        UNION
                        SELECT pif.idFuente
                        FROM p_identificador_fuente pif
                        INNER JOIN m_jci jci ON pif.valor = jci.issn_2
                        WHERE pif.tipo IN ('issn', 'eissn')
                    ) fuentes_con_metrica ON fuentes_con_metrica.idFuente = pf.idFuente
                    WHERE pp.eliminado = '0'
                    AND pf.idFuente IS NOT NULL
                    AND pf.eliminado = '0'
                    AND pf.tipo = 'Revista'
                    AND EXISTS (
                        SELECT 1 FROM p_identificador_publicacion pip
                        WHERE pip.idPublicacion = pp.idPublicacion
                        AND (pip.tipo = 'wos' OR pip.tipo = 'medline')
                    )
                    AND fuentes_con_metrica.idFuente IS NULL
                    ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones asociadas a una Revista con SJR/Citiscore y no tenga idScopus
def get_pub_Publicaciones_sjr_citescore_sin_scopus(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                            pp.idPublicacion        AS ID_PUB,
                            pp.tipo                 AS TIPO,
                            pp.titulo               AS TITULO,
                            pp.agno                 AS AGNO,
                            ic.idBiblioteca         AS ID_BIBLIOTECA,
                            ib.nombre               AS BIBLIOTECA,
                            pf.titulo               AS TITULO_FUENTE
                        FROM prisma.publicacionesXcentro pp
                        LEFT JOIN p_fuente pf ON pf.idFuente = pp.idFuente
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        LEFT JOIN (
                            SELECT pif.idFuente
                            FROM p_identificador_fuente pif
                            INNER JOIN m_sjr sjr ON pif.valor = sjr.issn
                            WHERE pif.tipo IN ('issn', 'eissn')
                            UNION
                            SELECT pif.idFuente
                            FROM p_identificador_fuente pif
                            INNER JOIN m_sjr sjr ON pif.valor = sjr.issn_2
                            WHERE pif.tipo IN ('issn', 'eissn')
                            UNION
                            SELECT pif.idFuente
                            FROM p_identificador_fuente pif
                            INNER JOIN m_citescore cs ON pif.valor = cs.issn
                            WHERE pif.tipo IN ('issn', 'eissn')
                        ) fuentes_con_metrica ON fuentes_con_metrica.idFuente = pf.idFuente
                        WHERE pp.eliminado = '0'
                        AND pf.idFuente IS NOT NULL
                        AND pf.eliminado = '0'
                        AND pf.tipo = 'Revista'
                        AND fuentes_con_metrica.idFuente IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM p_identificador_publicacion pip
                            WHERE pip.idPublicacion = pp.idPublicacion
                            AND pip.tipo = 'scopus'
                        )
                        ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de publicaciones asociadas a una Revista con JCR y no tenga idWOS
def get_pub_Publicaciones_jcr_jci_sin_wos(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                        pp.idPublicacion        AS ID_PUB,
                        pp.tipo                 AS TIPO,
                        pp.titulo               AS TITULO,
                        pp.agno                 AS AGNO,
                        ic.idBiblioteca         AS ID_BIBLIOTECA,
                        ib.nombre               AS BIBLIOTECA,
                        pf.titulo               AS TITULO_FUENTE
                    FROM prisma.publicacionesXcentro pp
                    LEFT JOIN p_fuente pf ON pf.idFuente = pp.idFuente
                    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                    LEFT JOIN (
                        SELECT pif.idFuente
                        FROM p_identificador_fuente pif
                        INNER JOIN m_jcr jcr ON pif.valor = jcr.issn
                        WHERE pif.tipo IN ('issn', 'eissn')
                        UNION
                        SELECT pif.idFuente
                        FROM p_identificador_fuente pif
                        INNER JOIN m_jcr jcr ON pif.valor = jcr.issn_2
                        WHERE pif.tipo IN ('issn', 'eissn')
                        UNION
                        SELECT pif.idFuente
                        FROM p_identificador_fuente pif
                        INNER JOIN m_jci jci ON pif.valor = jci.issn
                        WHERE pif.tipo IN ('issn', 'eissn')
                        UNION
                        SELECT pif.idFuente
                        FROM p_identificador_fuente pif
                        INNER JOIN m_jci jci ON pif.valor = jci.issn_2
                        WHERE pif.tipo IN ('issn', 'eissn')
                    ) fuentes_con_metrica ON fuentes_con_metrica.idFuente = pf.idFuente
                    WHERE pp.eliminado = '0'
                    AND pf.idFuente IS NOT NULL
                    AND pf.eliminado = '0'
                    AND pf.tipo = 'Revista'
                    AND fuentes_con_metrica.idFuente IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM p_identificador_publicacion pip
                        WHERE pip.idPublicacion = pp.idPublicacion
                        AND (pip.tipo = 'wos' OR pip.tipo = 'medline')
                    )
                    ORDER BY pp.idPublicacion DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Métricas
# Num Publicaciones por tipo y biblioteca
def get_pub_num_publicaciones_por_biblioteca(bd: BaseDatos = None) -> dict:
    query = """SELECT 
                ic.idBiblioteca         AS ID_BIBLIOTECA,
                ib.nombre               AS BIBLIOTECA,
                pp.tipo                 AS TIPO,
                COUNT(DISTINCT pp.idPublicacion) AS NUM_PUBLICACIONES
            FROM prisma.publicacionesXcentro pp
            LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
            LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
            WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis'
            GROUP BY ic.idBiblioteca, ib.nombre , pp.tipo
            ORDER BY ic.idBiblioteca, NUM_PUBLICACIONES DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# ****************************************
# ************ INVESTIGADORES *************
# ****************************************


# Lista de investigadores sin email
def get_inv_investigadores_sin_email(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                            i.idInvestigador    AS ID_INVES,
                            i.nombre            AS NOMBRE,
                            i.apellidos         AS APELLIDOS,
                            ic.idBiblioteca     AS ID_BIBLIOTECA,
                            ib.nombre           AS BIBLIOTECA,
                            id.nombre 			AS DEPARTAMENTO	
                        FROM i_investigador_activo i
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = i.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        LEFT JOIN prisma.i_departamento id ON id.idDepartamento = i.idDepartamento 
                        WHERE i.email = '' OR i.email IS NULL"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de investigadores sin identificadores
def get_inv_investigadores_sin_identificadores(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                            i.idInvestigador    AS ID_INVES,
                            i.nombre            AS NOMBRE,
                            i.apellidos         AS APELLIDOS,
                            ic.idBiblioteca     AS ID_BIBLIOTECA,
                            ib.nombre           AS BIBLIOTECA,
                            id.nombre 			AS DEPARTAMENTO	
                        FROM i_investigador_activo i
                        LEFT JOIN i_identificador_investigador ii ON ii.idInvestigador = i.idInvestigador
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = i.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        LEFT JOIN prisma.i_departamento id ON id.idDepartamento = i.idDepartamento 
                        WHERE ii.idInvestigador IS NULL"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de investigadores sin publicaciones
def get_inv_investigadores_sin_publicaciones(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                            i.idInvestigador    AS ID_INVES,
                            i.nombre            AS NOMBRE,
                            i.apellidos         AS APELLIDOS,
                            ic.idBiblioteca     AS ID_BIBLIOTECA,
                            ib.nombre           AS BIBLIOTECA,
                            id.nombre 			AS DEPARTAMENTO	
                        FROM i_investigador_activo i
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = i.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        LEFT JOIN prisma.i_departamento id ON id.idDepartamento = i.idDepartamento
                        WHERE i.idInvestigador NOT IN (
                            SELECT DISTINCT idInvestigador
                            FROM prisma.p_autor
                            WHERE idInvestigador IS NOT NULL
                            AND eliminado = '0'
                        )
                        ORDER BY ic.idBiblioteca, i.apellidos;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de los 20 últimos investigadores insertados
def get_inv_investigadores_20_ultimos_insertados(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT *
                            FROM (
                                SELECT
                                    i.idInvestigador    AS ID_INVES,
                                    i.nombre            AS NOMBRE,
                                    i.apellidos         AS APELLIDOS,
                                    ic.idBiblioteca     AS ID_BIBLIOTECA,
                                    ib.nombre           AS BIBLIOTECA,
                                    id.nombre 			AS DEPARTAMENTO,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY ic.idBiblioteca 
                                        ORDER BY i.idInvestigador DESC
                                    ) AS rn
                                FROM i_investigador_activo i
                                LEFT JOIN prisma.i_centro ic ON ic.idCentro = i.idCentro
                                LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                                LEFT JOIN prisma.i_departamento id ON id.idDepartamento = i.idDepartamento 
                            ) ranked
                            WHERE rn <= 20
                            ORDER BY ID_BIBLIOTECA, ID_INVES DESC;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# Lista de los investigadores sin ORCID
def get_inv_investigadores_sin_orcid(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                            i.idInvestigador    AS ID_INVES,
                            i.nombre            AS NOMBRE,
                            i.apellidos         AS APELLIDOS,
                            ic.idBiblioteca     AS ID_BIBLIOTECA,
                            ib.nombre           AS BIBLIOTECA,
                            id.nombre 			AS DEPARTAMENTO	
                        FROM i_investigador_activo i
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = i.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        LEFT JOIN prisma.i_departamento id ON id.idDepartamento = i.idDepartamento 
                        WHERE i.idInvestigador NOT IN (
                            SELECT DISTINCT idInvestigador
                            FROM i_identificador_investigador
                            WHERE tipo = 'orcid'
                            AND idInvestigador IS NOT NULL
                        )
                        ORDER BY ic.idBiblioteca, i.apellidos;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# ****************************************
# ************   FUENTES     *************
# ****************************************


# Lista de los investigadores sin ORCID
def get_inv_investigadores_sin_orcid(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT
                            i.idInvestigador    AS ID_INVES,
                            i.nombre            AS NOMBRE,
                            i.apellidos         AS APELLIDOS,
                            ic.idBiblioteca     AS ID_BIBLIOTECA,
                            ib.nombre           AS BIBLIOTECA,
                            id.nombre 			AS DEPARTAMENTO	
                        FROM i_investigador_activo i
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = i.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        LEFT JOIN prisma.i_departamento id ON id.idDepartamento = i.idDepartamento 
                        WHERE i.idInvestigador NOT IN (
                            SELECT DISTINCT idInvestigador
                            FROM i_identificador_investigador
                            WHERE tipo = 'orcid'
                            AND idInvestigador IS NOT NULL
                        )
                        ORDER BY ic.idBiblioteca, i.apellidos;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica
