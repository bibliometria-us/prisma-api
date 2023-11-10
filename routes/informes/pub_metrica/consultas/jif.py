from utils.timing import func_timer as timer
from db.conexion import BaseDatos
import routes.informes.config as config

select = [
    "CONCAT('https://prisma.us.es/publicacion/', p.idPublicacion) as 'URL Prisma'",

    # JIF
    "CAST(MAX(jif.impact_factor) as DOUBLE) AS 'JIF'",

    # --------- SCIE ------------

    # CATEGORÍAS SCIE
    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jif.edition = 'SCIE' THEN 
                    CONCAT(jif.category, ' (', jif.quartile,')')
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías JIF-SCIE'
    """,

    # CUARTILES SCIE
    "MIN(CASE WHEN jif.edition = 'SCIE' THEN jif.quartile ELSE NULL END) AS 'Mejor Cuartil JIF-SCIE'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jif.edition = 'SCIE' 
                    AND jif.quartile = (SELECT MIN(quartile) FROM m_jcr WHERE journal = jif.journal AND year = jif.year AND edition = jif.edition)
                    THEN jif.category
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Cuartil JIF-SCIE'
    """,

    # DECILES SCIE
    "MIN(CASE WHEN jif.edition = 'SCIE' THEN jif.decil ELSE NULL END) AS 'Mejor Decil JIF-SCIE'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jif.edition = 'SCIE' 
                    AND jif.decil = (SELECT MIN(decil) FROM m_jcr WHERE journal = jif.journal AND year = jif.year AND edition = jif.edition)
                    THEN jif.category
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Decil JIF-SCIE'
    """,

    # TERCILES SCIE
    "MIN(CASE WHEN jif.edition = 'SCIE' THEN jif.tercil ELSE NULL END) AS 'Mejor Tercil JIF-SCIE'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jif.edition = 'SCIE' 
                    AND jif.tercil = (SELECT MIN(tercil) FROM m_jcr WHERE journal = jif.journal AND year = jif.year AND edition = jif.edition)
                    THEN jif.category
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Tercil JIF-SCIE'
    """,

    # --------- SSCI ------------

    # CATEGORÍAS SSCI
    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jif.edition = 'SSCI' THEN 
                    CONCAT(jif.category, ' (', jif.quartile,')')
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías JIF-SSCI'
    """,

    # CUARTILES SSCI
    "MIN(CASE WHEN jif.edition = 'SSCI' THEN jif.quartile ELSE NULL END) AS 'Mejor Cuartil JIF-SSCI'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jif.edition = 'SSCI' 
                    AND jif.quartile = (SELECT MIN(quartile) FROM m_jcr WHERE journal = jif.journal AND year = jif.year AND edition = jif.edition)
                    THEN jif.category
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Cuartil JIF-SSCI'
    """,

    # DECILES SSCI
    "MIN(CASE WHEN jif.edition = 'SSCI' THEN jif.decil ELSE NULL END) AS 'Mejor Decil JIF-SSCI'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jif.edition = 'SSCI' 
                    AND jif.decil = (SELECT MIN(decil) FROM m_jcr WHERE journal = jif.journal AND year = jif.year AND edition = jif.edition)
                    THEN jif.category
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Decil JIF-SSCI'
    """,

    # TERCILES SSCI

    "MIN(CASE WHEN jif.edition = 'SSCI' THEN jif.tercil ELSE NULL END) AS 'Mejor Tercil JIF-SSCI'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jif.edition = 'SSCI' 
                    AND jif.tercil = (SELECT MIN(tercil) FROM m_jcr WHERE journal = jif.journal AND year = jif.year AND edition = jif.edition)
                    THEN jif.category
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Tercil JIF-SSCI'
    """,





]

joins = [
    # Fuente de la publicación
    "LEFT JOIN p_fuente f ON f.idFuente = p.idFuente",
    # Métricas JIF de la revista de la publicación
    f"LEFT JOIN m_jcr jif ON jif.idFuente = f.idFuente AND jif.year = LEAST(p.agno, {config.max_jcr_year})",


]


group_by = [
    "p.idPublicacion",
]

order_by = ["p.agno DESC",
            "p.idPublicacion"]


# @timer
def consulta_jif(publicaciones):
    query = f"SELECT {', '.join(select)} FROM p_publicacion p"
    query += f" {' '.join(joins)} "
    query += f" WHERE p.idPublicacion IN ({','.join(publicaciones)})"
    query += f" GROUP BY {','.join(group_by)}"
    query += f" ORDER BY {','.join(order_by)}"

    db = BaseDatos()
    params = []
    result = db.ejecutarConsulta(query, params)

    return result
