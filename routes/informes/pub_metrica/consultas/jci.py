from utils.timing import func_timer as timer
from db.conexion import BaseDatos
import routes.informes.config as config

select = [
    "CONCAT('https://prisma.us.es/publicacion/', p.idPublicacion) as 'URL Prisma'",

    # JCI
    "CAST(MAX(jci.jci) as FLOAT) AS 'JCI'",

    # CATEGORÍAS
    """
    GROUP_CONCAT(DISTINCT  
                CONCAT(jci.categoria, ' (', jci.cuartil,')')
               SEPARATOR ';')
                AS 'Categorías JCI'
    """,

    # CUARTILES
    "MIN(jci.cuartil) AS 'Mejor Cuartil JCI'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jci.cuartil = (SELECT MIN(cuartil) FROM m_jci WHERE revista = jci.revista AND agno = jci.agno)
                    THEN jci.categoria
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Cuartil JCI'
    """,

    # DECILES
    "MIN(jci.decil) AS 'Mejor Decil JCI'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jci.decil = (SELECT MIN(decil) FROM m_jci WHERE revista = jci.revista AND agno = jci.agno)
                    THEN jci.categoria
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Decil JCI'
    """,

    # TERCILES
    "MIN(jci.tercil) AS 'Mejor Tercil JCI'",

    """
    GROUP_CONCAT(DISTINCT 
                (CASE WHEN jci.tercil = (SELECT MIN(tercil) FROM m_jci WHERE revista = jci.revista AND agno = jci.agno)
                    THEN jci.categoria
                ELSE NULL END) SEPARATOR ';')
                AS 'Categorías Mejor Tercil JCI'
    """,


]

joins = [
    # Fuente de la publicación
    "LEFT JOIN p_fuente f ON f.idFuente = p.idFuente",
    # Métricas JCI de la revista de la publicación
    f"LEFT JOIN m_jci jci ON jci.idFuente = f.idFuente AND jci.agno = LEAST(p.agno, {config.max_jci_year})",


]


group_by = [
    "p.idPublicacion",
]

order_by = ["p.agno DESC",
            "p.idPublicacion"]


# @timer
def consulta_jci(publicaciones):
    query = f"SELECT {', '.join(select)} FROM p_publicacion p"
    query += f" {' '.join(joins)} "
    query += f" WHERE p.idPublicacion IN ({','.join(publicaciones)})"
    query += f" GROUP BY {','.join(group_by)}"
    query += f" ORDER BY {','.join(order_by)}"

    db = BaseDatos()
    params = []
    result = db.ejecutarConsulta(query, params)

    return result
