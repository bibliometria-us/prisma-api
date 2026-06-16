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
    query = """SELECT ic.idCentro AS ID_CENTRO, ic.nombre AS CENTRO FROM i_centro ic;"""
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
    query = """SELECT id.idDepartamento AS ID_DEPARTAMENTO, id.nombre AS DEPARTAMENTO FROM i_departamento id;"""
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
    query = """SELECT ia.idArea AS ID_AREA, ia.nombre AS AREA FROM i_area ia;"""
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
    query = """SELECT ii.idInstituto AS ID_INSTITUTO, ii.nombre AS INSTITUTO FROM i_instituto ii;"""
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
    query = """SELECT iue.idUdExcelencia AS ID_UD_EXCELENCIA, iue.nombre AS UD_EXCELENCIA FROM i_unidad_excelencia iue;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta

# Obtiene la lista de grupos
def get_grupos(bd: BaseDatos = None) -> dict:
    query = """SELECT ig.idGrupo AS ID_Grupo, ig.nombre AS GRUPO FROM i_grupo ig WHERE estado="Válido";"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


# Obtiene la lista de programas de doctorado
def get_programa_doctorado(bd: BaseDatos = None) -> dict:
    query = """SELECT id.idDoctorado AS ID_DOCTORADO, id.nombre AS DOCTORADO FROM i_doctorado id ;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query)
        consulta = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return consulta


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
# ************ PUBLICACIONES *************
# ****************************************

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

# Obtiene la lista de los tipo permitidos de fuentes (incluidos en la tabla de configuración de tipos de fuente)
def get_tipos_fuente_permitidos(bd: BaseDatos = None) -> dict:
    query = (
        """SELECT tf.nombre FROM config.tipos_fuente tf WHERE tf.activo = '1'"""
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
    WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis' AND pp.tipo NOT IN (SELECT nombre FROM config.tipos_publicacion WHERE activo = '1')
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
    # query_publicacion = """SELECT
    #                     pp.idPublicacion        AS ID_PUB,
    #                     pp.titulo               AS TITULO,
    #                     pp.tipo               AS TIPO,
    #                     pp.agno                 AS AGNO,
    #                     pip.tipo                AS TIPO_ID,
    #                     ic.idBiblioteca         AS ID_BIBLIOTECA,
    #                     ib.nombre               AS BIBLIOTECA
    #                 FROM prisma.publicacionesXcentro pp
    #                 LEFT JOIN p_identificador_publicacion pip ON pip.idPublicacion = pp.idPublicacion
    #                 LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
    #                 LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
    #                 WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis'
    #                 AND (pp.idPublicacion, pip.tipo) IN (
    #                     SELECT idPublicacion, tipo
    #                     FROM p_identificador_publicacion
    #                     GROUP BY idPublicacion, tipo
    #                     HAVING COUNT(*) > 1
    #                 )
    #                 GROUP BY pp.idPublicacion, pp.titulo, pp.agno, pip.tipo, ic.idBiblioteca, ib.nombre
    #                 ORDER BY pp.idPublicacion DESC, pip.tipo;"""
    query_publicacion = """SELECT 
                        pp.idPublicacion        AS ID_PUB,
                        pp.titulo               AS TITULO,
                        pp.tipo                 AS TIPO,
                        pp.agno                 AS AGNO,
                        pip.tipo                AS TIPO_ID,
                        pip.valor               AS VALOR_ID,
                        SUBSTRING_INDEX(pip.valor, ':', 1) AS PREFIJO,
                        ic.idBiblioteca         AS ID_BIBLIOTECA,
                        ib.nombre               AS BIBLIOTECA
                    FROM prisma.publicacionesXcentro pp
                    LEFT JOIN p_identificador_publicacion pip ON pip.idPublicacion = pp.idPublicacion
                    LEFT JOIN prisma.i_centro ic ON ic.idCentro = pp.idCentro
                    LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                    WHERE pp.eliminado = '0' AND pp.tipo != 'Tesis'
                    AND (pp.idPublicacion, pip.tipo, SUBSTRING_INDEX(pip.valor, ':', 1)) IN (
                        SELECT idPublicacion, tipo, SUBSTRING_INDEX(valor, ':', 1)
                        FROM p_identificador_publicacion
                        GROUP BY idPublicacion, tipo, SUBSTRING_INDEX(valor, ':', 1)
                        HAVING COUNT(*) > 1
                    )
                    GROUP BY pp.idPublicacion, pp.titulo, pp.agno, pip.tipo, SUBSTRING_INDEX(pip.valor, ':', 1), ic.idBiblioteca, ib.nombre
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
                                WHERE idInvestigador > '0'
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


# Lista de publicaciones sin autores
def get_pub_publicaciones_sin_autores(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pp.idPublicacion AS ID_PUB, pp.tipo AS TIPO, pp.titulo AS TITULO,
                            pp.agno AS AGNO
                        FROM prisma.p_publicacion pp
                        WHERE pp.eliminado = '0' 
                        AND pp.tipo != 'Tesis'
                        AND pp.idPublicacion NOT IN (
                            SELECT DISTINCT idPublicacion
                            FROM prisma.p_autor
                            WHERE eliminado = '0'
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
                        FROM p_dato_publicacion WHERE tipo != 'titulo_alt'
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
def get_pub_publicaciones_sjr_citescore_sin_scopus(bd: BaseDatos = None) -> dict:
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
def get_pub_publicaciones_jcr_jci_sin_wos(bd: BaseDatos = None) -> dict:
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


def get_fuentes_sin_identificadores(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT  pf.idFuente   AS ID_FUENTE,     
                                    pf.titulo            AS TITULO,
                                    pf.tipo             AS TIPO
                            FROM p_fuente pf
                            WHERE pf.eliminado = 0 AND pf.idFuente != 0 AND pf.tipo != 'Congreso'
                            AND pf.idFuente NOT IN (
                                SELECT pif.idFuente 
                                FROM p_identificador_fuente pif
                                WHERE pif.eliminado = 0
                            )
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_fuentes_sin_tipo_admitido(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT  pf.idFuente   AS ID_FUENTE,     
                                pf.titulo            AS TITULO,
                                pf.tipo             AS TIPO
                        FROM p_fuente pf
                        WHERE pf.eliminado = 0 AND pf.idFuente != 0 
                        AND pf.tipo NOT IN (SELECT tf.nombre FROM config.tipos_fuente tf WHERE tf.activo = '1')
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_fuentes_coleccion_con_issn_y_isbn(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT  
                            pf.idFuente   AS ID_FUENTE,     
                            pf.titulo     AS TITULO,
                            pf.tipo       AS TIPO
                        FROM p_fuente pf
                        WHERE pf.eliminado = 0 AND pf.idFuente != 0 AND pf.tipo = 'Coleccion'
                        AND pf.idFuente IN (
                            SELECT pif.idFuente 
                            FROM p_identificador_fuente pif
                            WHERE pif.eliminado = 0
                            AND pif.tipo IN ('issn', 'eissn')
                            GROUP BY pif.idFuente
                            HAVING COUNT(*) > 0
                        )
                        AND pf.idFuente IN (
                            SELECT pif.idFuente 
                            FROM p_identificador_fuente pif
                            WHERE pif.eliminado = 0
                            AND pif.tipo IN ('isbn', 'eisbn')
                            GROUP BY pif.idFuente
                            HAVING COUNT(*) > 0
                        )
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_fuentes_dato_coleccion_enlazado_a_fuente_no_coleccion(
    bd: BaseDatos = None,
) -> dict:
    query_publicacion = """SELECT 
                            pf.idFuente   AS ID_FUENTE,
                            pf.titulo     AS TITULO,
                            pf.tipo       AS TIPO
                        FROM p_fuente pf
                        JOIN p_dato_fuente pdf 
                            ON pdf.idFuente = pf.idFuente 
                            AND pdf.tipo = 'coleccion'
                        JOIN p_fuente pf_col 
                            ON pf_col.idFuente = pdf.valor
                            AND pf_col.tipo != 'coleccion'
                        WHERE pf.eliminado = 0
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_fuentes_con_coleccion_a_si_misma(
    bd: BaseDatos = None,
) -> dict:
    query_publicacion = """SELECT 
                        pf.idFuente   AS ID_FUENTE,
                        pf.titulo     AS TITULO,
                        pf.tipo       AS TIPO
                    FROM p_fuente pf
                    JOIN p_dato_fuente pdf 
                        ON pdf.idFuente = pf.idFuente 
                        AND pdf.tipo = 'coleccion'
                    WHERE pf.eliminado = 0
                    AND pdf.valor = pf.idFuente """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_fuentes_no_tipo_libro_con_colecciones(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                                pf.idFuente   AS ID_FUENTE,
                                pf.titulo     AS TITULO,
                                pf.tipo       AS TIPO
                            FROM p_fuente pf
                            JOIN p_dato_fuente pdf 
                                ON pdf.idFuente = pf.idFuente 
                                AND pdf.tipo = 'coleccion'
                            JOIN p_fuente pf_col 
                                ON pf_col.idFuente = pdf.valor
                            WHERE pf.eliminado = 0
                            AND pf.tipo != 'libro'"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_fuente_sin_publicaciones_no_APC(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT pf.idFuente, pf.titulo, pf.tipo
                        FROM p_fuente pf
                        WHERE pf.eliminado = 0
                        AND pf.tipo = 'revista'
                        AND pf.idFuente NOT IN (
                            SELECT DISTINCT idFuente
                            FROM p_publicacion
                            WHERE eliminado = 0
                        )
                        AND pf.idFuente NOT IN (
                            SELECT DISTINCT idFuente
                            FROM m_at
                        )"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_fuente_con_issn_e_isbn(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT  
                            pf.idFuente   AS ID_FUENTE,     
                            pf.titulo     AS TITULO,
                            pf.tipo       AS TIPO
                        FROM p_fuente pf
                        WHERE pf.eliminado = 0 
                        AND pf.idFuente != 0 
                        AND pf.tipo != 'Coleccion'
                        AND pf.idFuente IN (
                            SELECT pif.idFuente 
                            FROM p_identificador_fuente pif
                            WHERE pif.eliminado = 0
                            AND pif.tipo IN ('issn', 'eissn')
                        )
                        AND pf.idFuente IN (
                            SELECT pif.idFuente 
                            FROM p_identificador_fuente pif
                            WHERE pif.eliminado = 0
                            AND pif.tipo IN ('isbn', 'eisbn')
                        )"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica

def get_fuentes_APC_no_activas(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT ma.idFuente AS ID_FUENTE, NULL AS TITULO, NULL AS TIPO, 'No existe en p_fuente' AS MOTIVO
                            FROM m_at ma
                            WHERE ma.idFuente NOT IN (
                                SELECT idFuente FROM p_fuente
                            )
                            AND ma.agno = YEAR(CURDATE())
                            UNION ALL

                            SELECT pf.idFuente AS ID_FUENTE, pf.titulo AS TITULO, pf.tipo AS TIPO, 'Fuente eliminada' AS MOTIVO
                            FROM p_fuente pf
                            WHERE pf.eliminado = 1
                            AND pf.idFuente IN (
                                SELECT DISTINCT idFuente FROM m_at WHERE agno = YEAR(CURDATE())
                            )"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica

def get_fuentes_identificador_repetido(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                        pif.tipo                                    AS TIPO_ID,
                        pif.valor                                   AS VALOR,
                        COUNT(DISTINCT pif.idFuente)                AS NUM_FUENTES,
                        GROUP_CONCAT(DISTINCT pif.idFuente)         AS IDS_FUENTE,
                        GROUP_CONCAT(DISTINCT pf.titulo)            AS TITULOS
                    FROM p_identificador_fuente pif
                    JOIN p_fuente pf ON pf.idFuente = pif.idFuente
                    WHERE pif.eliminado = 0
                    AND pf.eliminado = 0 AND pif.tipo IN ('issn', 'eissn', 'isbn', 'eisbn', 'doi', 'wos')
                    GROUP BY pif.tipo, pif.valor
                    HAVING COUNT(DISTINCT pif.idFuente) > 1
                    ORDER BY pif.tipo, pif.valor;"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica

# ****************************************
# ************   PROYECTOS   *************
# ****************************************


def get_proyectos_referencia_nula_o_menos_5_caracteres(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT   
                            pp.id               AS ID_PROYECTO,      
                            pp.nombre           AS NOMBRE,
                            pp.tipo             AS TIPO,
                            pp.referencia       AS REFERENCIA
                        FROM prisma_proyectos.proyecto pp
                        WHERE (pp.referencia IS NULL OR LENGTH(TRIM(pp.referencia)) < 5) AND pp.visible != '0';
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_proyectos_importe_nulo_menor_100(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT   
                            pp.id               AS ID_PROYECTO,      
                            pp.nombre           AS NOMBRE,
                            pp.tipo             AS TIPO,
                            pp.concedido       AS CONCEDIDO 
                            FROM prisma_proyectos.proyecto pp
                        WHERE (pp.concedido < 100 OR pp.concedido IS NULL) AND pp.visible != '0'"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


# ****************************************
# ************   FINANCIACION   *************
# ****************************************


def get_financiacion_codigo_nulo_o_menos_4_caracteres(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                            sub.ID_FINANCIACION,
                            sub.CODIGO,
                            sub.ID_PUB,
                            sub.TITULO,
                            sub.AGNO,
                            MIN(ic.idBiblioteca)    AS ID_BIBLIOTECA,
                            MIN(ib.nombre)          AS BIBLIOTECA
                        FROM (
                            SELECT 
                                pf.idFinanciacion       AS ID_FINANCIACION,      
                                pf.codigo               AS CODIGO,
                                pf.publicacion_id       AS ID_PUB,
                                pp.titulo               AS TITULO,
                                pp.agno                 AS AGNO
                            FROM prisma.p_financiacion pf
                            LEFT JOIN prisma.p_publicacion pp ON pp.idPublicacion = pf.publicacion_id
                            WHERE (pf.codigo IS NULL OR LENGTH(TRIM(pf.codigo)) < 4)
                        ) sub
                        LEFT JOIN prisma.publicacionesXcentro pxc ON pxc.idPublicacion = sub.ID_PUB
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = pxc.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        GROUP BY sub.ID_FINANCIACION, sub.CODIGO, sub.ID_PUB, sub.TITULO
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_financiacion_agencia_nula_o_menos_5_caracteres(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                            sub.ID_FINANCIACION,
                            sub.AGENCIA,
                            sub.ID_PUB,
                            sub.TITULO,
                            sub.AGNO,
                            MIN(ic.idBiblioteca)    AS ID_BIBLIOTECA,
                            MIN(ib.nombre)          AS BIBLIOTECA
                        FROM (
                            SELECT 
                                pf.idFinanciacion       AS ID_FINANCIACION,      
                                pf.agencia              AS AGENCIA,
                                pf.publicacion_id       AS ID_PUB,
                                pp.titulo               AS TITULO,
                                pp.agno                 AS AGNO
                            FROM prisma.p_financiacion pf
                            LEFT JOIN prisma.p_publicacion pp ON pp.idPublicacion = pf.publicacion_id
                            WHERE (pf.agencia IS NULL OR LENGTH(TRIM(pf.agencia)) < 5)
                        ) sub
                        LEFT JOIN prisma.publicacionesXcentro pxc ON pxc.idPublicacion = sub.ID_PUB
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = pxc.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
                        GROUP BY sub.ID_FINANCIACION, sub.AGENCIA, sub.ID_PUB, sub.TITULO
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_financiacion_repetida_por_publicacion(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                            duplicados.ID_FINANCIACION,
                            duplicados.CODIGO,
                            duplicados.AGENCIA,
                            duplicados.ID_PUB,
                            duplicados.REPETICIONES,
                            duplicados.TITULO,
                            ic.idBiblioteca         AS ID_BIBLIOTECA,
                            ib.nombre               AS BIBLIOTECA
                        FROM (
                            SELECT 
                                MIN(pf.idFinanciacion)  AS ID_FINANCIACION,
                                pf.codigo               AS CODIGO,
                                pf.agencia              AS AGENCIA,
                                pf.publicacion_id       AS ID_PUB,
                                COUNT(*)                AS REPETICIONES,
                                pp.titulo               AS TITULO
                            FROM prisma.p_financiacion pf
                            LEFT JOIN prisma.p_publicacion pp ON pp.idPublicacion = pf.publicacion_id
                            GROUP BY pf.codigo, pf.agencia, pf.publicacion_id
                            HAVING COUNT(*) > 1
                        ) duplicados
                        LEFT JOIN prisma.publicacionesXcentro pxc ON pxc.idPublicacion = duplicados.ID_PUB
                        LEFT JOIN prisma.i_centro ic ON ic.idCentro = pxc.idCentro
                        LEFT JOIN prisma.i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca"""
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica

#--------- Metricas financiacion-------

def get_num_proyectos_con_financiacion(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                            COUNT(DISTINCT pf.idProyecto)    AS PROYECTOS_CON_FINANCIACION,
                            (SELECT COUNT(*) FROM prisma_proyectos.proyecto) AS TOTAL_PROYECTOS
                        FROM prisma_proyectos.proyecto pr
                        INNER JOIN prisma.p_financiacion pf ON pf.idProyecto = pr.id AND pr.visible = 1
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


def get_num_financiacion_con_proyectos(bd: BaseDatos = None) -> dict:
    query_publicacion = """SELECT 
                                COUNT(DISTINCT pf_con.idFinanciacion)                           AS FINANCIACIONES_CON_PROYECTO,
                                (SELECT COUNT(*) FROM prisma.p_financiacion) AS TOTAL_FINANCIACIONES
                            FROM prisma.p_financiacion pf_con
                            INNER JOIN prisma_proyectos.proyecto pr ON pr.id = pf_con.idProyecto
                            AND pr.visible = 1
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica


#############################################
################# RANKING ###################
#############################################

def get_listado_metricas_investigadores(bd: BaseDatos = None) -> dict:
    query_publicacion = """
                        SELECT 
                            iia.nombre              AS NOMBRE,
                            iia.apellidos           AS APELLIDOS,
                            id.idDepartamento       AS ID_DEPARTAMENTO,
                            id.nombre               AS DEPARTAMENTO,
                            iia.idCategoria         AS ID_CATEGORIA,
                            ic2.nombre              AS CATEGORIA,
                            ig.idGrupo              AS ID_GRUPO,
                            ig.nombre               AS GRUPO,
                            ic.idCentro             AS ID_CENTRO,
                            ic.nombre               AS CENTRO,
                            ia.idArea               AS ID_AREA,
                            ia.nombre               AS AREA,
                            ii.idInstituto          AS ID_INSTITUTO,
                            ii.nombre               AS INSTITUTO,
                            iue.idUdExcelencia      AS ID_UDEXELENCIA,
                            iue.nombre              AS UDEXELENCIA,
                            ipd.idDoctorado         AS ID_DOCTORANDO,
                            id2.nombre              AS DOCTORANDO,
                            iia.idInvestigador      AS Id_Prisma,
                            ids.Id_Scopus,
                            ids.Id_Wos,
                            ids.Id_Openalex,
                            met.N_Pubs_Total_Scopus,
                            met.N_Total_Citas_Total_Scopus,
                            met.Indice_H_Total_Scopus,
                            met.N_Pubs_10_Scopus,
                            met.N_Total_Citas_10_Scopus,
                            met.Indice_H_10_Scopus,
                            met.N_Pubs_5_Scopus,
                            met.N_Total_Citas_5_Scopus,
                            met.Indice_H_5_Scopus,
                            met.N_Pubs_3_Scopus,
                            met.N_Total_Citas_3_Scopus,
                            met.Indice_H_3_Scopus,
                            met.N_Pubs_Total_Wos,
                            met.N_Total_Citas_Total_Wos,
                            met.Indice_H_Total_Wos,
                            met.N_Pubs_10_Wos,
                            met.N_Total_Citas_10_Wos,
                            met.Indice_H_10_Wos,
                            met.N_Pubs_5_Wos,
                            met.N_Total_Citas_5_Wos,
                            met.Indice_H_5_Wos,
                            met.N_Pubs_3_Wos,
                            met.N_Total_Citas_3_Wos,
                            met.Indice_H_3_Wos,
                            ir.nombre               AS Rama
                        FROM i_investigador_activo iia
                        LEFT JOIN i_departamento id                 ON id.idDepartamento = iia.idDepartamento
                        LEFT JOIN i_categoria ic2                   ON ic2.idCategoria = iia.idCategoria
                        LEFT JOIN i_rama_us iru                     ON iru.idDepartamento = id.idDepartamento AND iru.idArea = iia.idArea
                        LEFT JOIN i_rama ir                         ON ir.idRama = iru.idRama
                        LEFT JOIN i_grupo_investigador igi          ON igi.idInvestigador = iia.idInvestigador
                        LEFT JOIN i_grupo ig                        ON ig.idGrupo = igi.idGrupo
                        LEFT JOIN i_centro ic                       ON ic.idCentro = iia.idCentro
                        LEFT JOIN i_area ia                         ON ia.idArea = iia.idArea
                        LEFT JOIN i_miembro_instituto imi           ON imi.idInvestigador = iia.idInvestigador
                        LEFT JOIN i_instituto ii                    ON ii.idInstituto = imi.idInstituto
                        LEFT JOIN i_miembro_unidad_excelencia imue  ON imue.idInvestigador = iia.idInvestigador
                        LEFT JOIN i_unidad_excelencia iue           ON imue.idUdExcelencia = iue.idUdExcelencia
                        LEFT JOIN i_profesor_doctorado ipd          ON ipd.idInvestigador = iia.idInvestigador
                        LEFT JOIN i_doctorado id2                   ON id2.idDoctorado = ipd.idDoctorado
                        LEFT JOIN (
                            SELECT idInvestigador,
                                MAX(CASE WHEN tipo = 'scopus'       THEN valor END) AS Id_Scopus,
                                MAX(CASE WHEN tipo = 'researcherId' THEN valor END) AS Id_Wos,
                                MAX(CASE WHEN tipo = 'openalex'     THEN valor END) AS Id_Openalex
                            FROM i_identificador_investigador
                            GROUP BY idInvestigador
                        ) ids ON ids.idInvestigador = iia.idInvestigador
                        LEFT JOIN (
                            SELECT identificadorInt,
                                MAX(CASE WHEN tipo = 'num_pub'      AND basedatos = 'scopus' THEN valor END) AS N_Pubs_Total_Scopus,
                                MAX(CASE WHEN tipo = 't_citas'      AND basedatos = 'scopus' THEN valor END) AS N_Total_Citas_Total_Scopus,
                                MAX(CASE WHEN tipo = 'indice_h'     AND basedatos = 'scopus' THEN valor END) AS Indice_H_Total_Scopus,
                                MAX(CASE WHEN tipo = 'num_pub_10'   AND basedatos = 'scopus' THEN valor END) AS N_Pubs_10_Scopus,
                                MAX(CASE WHEN tipo = 't_citas_10'   AND basedatos = 'scopus' THEN valor END) AS N_Total_Citas_10_Scopus,
                                MAX(CASE WHEN tipo = 'indice_h_10'  AND basedatos = 'scopus' THEN valor END) AS Indice_H_10_Scopus,
                                MAX(CASE WHEN tipo = 'num_pub_5'    AND basedatos = 'scopus' THEN valor END) AS N_Pubs_5_Scopus,
                                MAX(CASE WHEN tipo = 't_citas_5'    AND basedatos = 'scopus' THEN valor END) AS N_Total_Citas_5_Scopus,
                                MAX(CASE WHEN tipo = 'indice_h_5'   AND basedatos = 'scopus' THEN valor END) AS Indice_H_5_Scopus,
                                MAX(CASE WHEN tipo = 'num_pub_3'    AND basedatos = 'scopus' THEN valor END) AS N_Pubs_3_Scopus,
                                MAX(CASE WHEN tipo = 't_citas_3'    AND basedatos = 'scopus' THEN valor END) AS N_Total_Citas_3_Scopus,
                                MAX(CASE WHEN tipo = 'indice_h_3'   AND basedatos = 'scopus' THEN valor END) AS Indice_H_3_Scopus,
                                MAX(CASE WHEN tipo = 'num_pub'      AND basedatos = 'wos'    THEN valor END) AS N_Pubs_Total_Wos,
                                MAX(CASE WHEN tipo = 't_citas'      AND basedatos = 'wos'    THEN valor END) AS N_Total_Citas_Total_Wos,
                                MAX(CASE WHEN tipo = 'indice_h'     AND basedatos = 'wos'    THEN valor END) AS Indice_H_Total_Wos,
                                MAX(CASE WHEN tipo = 'num_pub_10'   AND basedatos = 'wos'    THEN valor END) AS N_Pubs_10_Wos,
                                MAX(CASE WHEN tipo = 't_citas_10'   AND basedatos = 'wos'    THEN valor END) AS N_Total_Citas_10_Wos,
                                MAX(CASE WHEN tipo = 'indice_h_10'  AND basedatos = 'wos'    THEN valor END) AS Indice_H_10_Wos,
                                MAX(CASE WHEN tipo = 'num_pub_5'    AND basedatos = 'wos'    THEN valor END) AS N_Pubs_5_Wos,
                                MAX(CASE WHEN tipo = 't_citas_5'    AND basedatos = 'wos'    THEN valor END) AS N_Total_Citas_5_Wos,
                                MAX(CASE WHEN tipo = 'indice_h_5'   AND basedatos = 'wos'    THEN valor END) AS Indice_H_5_Wos,
                                MAX(CASE WHEN tipo = 'num_pub_3'    AND basedatos = 'wos'    THEN valor END) AS N_Pubs_3_Wos,
                                MAX(CASE WHEN tipo = 't_citas_3'    AND basedatos = 'wos'    THEN valor END) AS N_Total_Citas_3_Wos,
                                MAX(CASE WHEN tipo = 'indice_h_3'   AND basedatos = 'wos'    THEN valor END) AS Indice_H_3_Wos
                            FROM m_informes
                            WHERE ambito = 'investigador'
                            GROUP BY identificadorInt
                        ) met ON met.identificadorInt = iia.idInvestigador
                        ORDER BY iia.idInvestigador
                        """
    try:
        if bd is None:
            bd = BaseDatos()
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe()
    except Exception as e:
        return {"error": e.message}, 400
    return metrica