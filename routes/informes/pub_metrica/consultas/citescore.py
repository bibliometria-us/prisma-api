from utils.timing import func_timer as timer
from db.conexion import BaseDatos
import routes.informes.config as config

select = [
    "CONCAT('https://prisma.us.es/publicacion/', p.idPublicacion) as 'URL Prisma'",
    # CiteScore
    "CAST(MAX(citescore.citescore) as DOUBLE) AS 'CiteScore'",
    # CATEGORÍAS
    """
    GROUP_CONCAT(DISTINCT  
                CONCAT(citescore.categoria, ' (', citescore.cuartil,')')
               SEPARATOR ';')
                AS 'Categorías CiteScore'
    """,
    # CUARTILES
    "MIN(citescore.cuartil) AS 'Mejor Cuartil CiteScore'",
    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN citescore.cuartil = (SELECT MIN(cuartil) FROM m_citescore WHERE revista = citescore.revista AND agno = citescore.agno)
                    THEN citescore.categoria
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Cuartil CiteScore'
    """,
    # DECILES
    "MIN(citescore.decil) AS 'Mejor Decil CiteScore'",
    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN citescore.decil = (SELECT MIN(decil) FROM m_citescore WHERE revista = citescore.revista AND agno = citescore.agno)
                    THEN citescore.categoria
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Decil CiteScore'
    """,
    # TERCILES
    "MIN(citescore.tercil) AS 'Mejor Tercil CiteScore'",
    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN citescore.tercil = (SELECT MIN(tercil) FROM m_citescore WHERE revista = citescore.revista AND agno = citescore.agno)
                    THEN citescore.categoria
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Tercil CiteScore'
    """,
]

joins = [
    # Fuente de la publicación
    "LEFT JOIN p_fuente f ON f.idFuente = p.idFuente",
    # Métricas CiteScore de la revista de la publicación
    f"LEFT JOIN m_citescore citescore ON citescore.idFuente = f.idFuente AND citescore.agno = LEAST(p.agno, {config.max_jci_year})",
]


group_by = [
    "p.idPublicacion",
]

order_by = ["p.agno DESC", "p.idPublicacion"]


# @timer
def consulta_citescore(publicaciones):
    query = f"SELECT {', '.join(select)} FROM p_publicacion p"
    query += f" {' '.join(joins)} "
    query += f" WHERE p.idPublicacion IN ({','.join(publicaciones)})"
    query += f" GROUP BY {','.join(group_by)}"
    query += f" ORDER BY {','.join(order_by)}"

    db = BaseDatos()
    params = []
    result = db.ejecutarConsulta(query, params)

    return result
