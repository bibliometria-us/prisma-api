from utils.timing import func_timer as timer
from db.conexion import BaseDatos
import routes.informes.config as config

select = [
    "CONCAT('https://prisma.us.es/publicacion/', p.idPublicacion) as 'URL Prisma'",
    # SJR
    "CAST(MAX(sjr.impact_factor) as DOUBLE) AS 'SJR'",
    # CATEGORÍAS
    """
    GROUP_CONCAT(DISTINCT  
                CONCAT(sjr.category, ' (', sjr.quartile,')')
               SEPARATOR ';')
                AS 'Categorías SJR'
    """,
    # CUARTILES
    "MIN(sjr.quartile) AS 'Mejor Cuartil SJR'",
    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN sjr.quartile = (SELECT MIN(quartile) FROM m_sjr WHERE journal = sjr.journal AND year = sjr.year)
                    THEN sjr.category
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Cuartil SJR'
    """,
    # DECILES
    "MIN(sjr.decil) AS 'Mejor Decil SJR'",
    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN sjr.decil = (SELECT MIN(decil) FROM m_sjr WHERE journal = sjr.journal AND year = sjr.year)
                    THEN sjr.category
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Decil SJR'
    """,
    # TERCILES
    "MIN(sjr.tercil) AS 'Mejor Tercil SJR'",
    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN sjr.tercil = (SELECT MIN(tercil) FROM m_sjr WHERE journal = sjr.journal AND year = sjr.year)
                    THEN sjr.category
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Tercil SJR'
    """,
]

joins = [
    # Fuente de la publicación
    "LEFT JOIN p_fuente f ON f.idFuente = p.idFuente",
    # Métricas SJR de la revista de la publicación
    f"LEFT JOIN m_sjr sjr ON sjr.idFuente = f.idFuente AND sjr.year = LEAST(p.agno, {config.max_jcr_year})",
]


group_by = [
    "p.idPublicacion",
]

order_by = ["p.agno DESC", "p.idPublicacion"]


# @timer
def consulta_sjr(publicaciones):
    query = f"SELECT {', '.join(select)} FROM p_publicacion p"
    query += f" {' '.join(joins)} "
    query += f" WHERE p.idPublicacion IN ({','.join(publicaciones)})"
    query += f" GROUP BY {','.join(group_by)}"
    query += f" ORDER BY {','.join(order_by)}"

    db = BaseDatos()
    params = []
    result = db.ejecutarConsulta(query, params)

    return result
