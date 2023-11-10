from utils.timing import func_timer as timer
from db.conexion import BaseDatos
import routes.informes.config as config

select = [
    "CONCAT('https://prisma.us.es/publicacion/', p.idPublicacion) as 'URL Prisma'",

    # IDR
    "CAST(MAX(idr.factorImpacto) as DOUBLE) AS 'IDR'",

    # CATEGORÍAS
    """
    GROUP_CONCAT(DISTINCT  
                CONCAT(idr.categoria, ' (', idr.cuartil,')')
               SEPARATOR ';')
                AS 'Categorías IDR'
    """,

    # CUARTILES
    "MIN(idr.cuartil) AS 'Mejor Cuartil IDR'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN idr.cuartil = (SELECT MIN(cuartil) FROM m_idr WHERE titulo = idr.titulo AND anualidad = idr.anualidad)
                    THEN idr.categoria
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Cuartil IDR'
    """,

    # PERCENTILES
    "MIN(idr.percentil) AS 'Mejor Percentil IDR'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN idr.percentil = (SELECT MIN(percentil) FROM m_idr WHERE titulo = idr.titulo AND anualidad = idr.anualidad)
                    THEN idr.categoria
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Percentil IDR'
    """,


]

joins = [
    # Fuente de la publicación
    "LEFT JOIN p_fuente f ON f.idFuente = p.idFuente",
    # Métricas IDR de la revista de la publicación
    f"LEFT JOIN m_idr idr ON idr.idFuente = f.idFuente AND idr.anualidad = LEAST(p.agno, {config.max_idr_year})",


]


group_by = [
    "p.idPublicacion",
]

order_by = ["p.agno DESC",
            "p.idPublicacion"]


# @timer
def consulta_idr(publicaciones):
    query = f"SELECT {', '.join(select)} FROM p_publicacion p"
    query += f" {' '.join(joins)} "
    query += f" WHERE p.idPublicacion IN ({','.join(publicaciones)})"
    query += f" GROUP BY {','.join(group_by)}"
    query += f" ORDER BY {','.join(order_by)}"

    db = BaseDatos()
    params = []
    result = db.ejecutarConsulta(query, params)

    return result
