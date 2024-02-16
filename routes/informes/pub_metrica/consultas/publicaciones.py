# CONSULTA PARA LOS DATOS DE PUBLICACIONES Y CITAS DE CADA PUBLICACIÓN
from utils.timing import func_timer as timer
from db.conexion import BaseDatos
from routes.informes.utils import format_query

count = ["COUNT(*) AS"]

select = [
    # Datos de publicación
    "p.idPublicacion",
    "CONCAT('https://prisma.us.es/publicacion/', p.idPublicacion) as 'URL Prisma'",
    "p.agno as 'Año de Publicación'",
    "p.tipo as 'Tipo'",
    "p.titulo as 'Título'",
    "fuente.titulo as 'Fuente'",

    # Lista de ISSN e ISBN
    "GROUP_CONCAT(DISTINCT CASE WHEN idfuente.tipo IN ('issn', 'eissn') AND idfuente.eliminado = 0 THEN idfuente.valor ELSE NULL END SEPARATOR ';') AS 'ISSN'",
    "GROUP_CONCAT(DISTINCT CASE WHEN idfuente.tipo IN ('isbn', 'eisbn') AND idfuente.eliminado = 0 THEN idfuente.valor ELSE NULL END SEPARATOR ';') AS 'ISBN'",

    # Diferenciar si una publicación tiene o no autoría grupal exclusiva
    "CASE WHEN COUNT(CASE WHEN autor_inf.rol != 'Grupo' THEN 1 ELSE NULL END) = 0 THEN 'Sí' ELSE 'No' END as 'Autoría Grupal Exclusiva Sí/No'",

    # Mostrar los datos de una publicación con formato
    """
    CONCAT(
        MAX(dp_volumen.valor),
        CASE
            WHEN MAX(dp_numero.valor) IS NOT NULL THEN
                CASE WHEN MAX(dp_volumen.valor) IS NOT NULL THEN CONCAT(' (', MAX(dp_numero.valor), '), ') ELSE CONCAT('(', MAX(dp_numero.valor), '), ') END
            ELSE
                CASE WHEN MAX(dp_volumen.valor) IS NOT NULL THEN ', ' ELSE '' END
        END,
        CASE
            WHEN MAX(dp_pag_inicio.valor) IS NOT NULL AND MAX(dp_pag_fin.valor) IS NOT NULL THEN CONCAT(MAX(dp_pag_inicio.valor), '-', MAX(dp_pag_fin.valor))
            WHEN MAX(dp_pag_inicio.valor) IS NOT NULL THEN MAX(dp_pag_inicio.valor)
            ELSE ''
        END
    ) AS 'Datos de la Publicación'
    """,

    "fuente.editorial as 'Editorial'",

    # Autores
    "GROUP_CONCAT(DISTINCT CASE WHEN autor.rol != 'Grupo' THEN autor.firma ELSE NULL END SEPARATOR ';') as 'Autores'",
    "GROUP_CONCAT(DISTINCT CASE WHEN autor.rol = 'Grupo' THEN autor.firma ELSE NULL END SEPARATOR ';') as 'Autores grupales'",
    "GROUP_CONCAT(DISTINCT CASE WHEN autor.rol != 'Grupo' AND autor.idInvestigador != 0 THEN CONCAT(autor.firma, ' [', autor.idInvestigador, ']') END SEPARATOR ';') as 'Autores US'",
    "GROUP_CONCAT(DISTINCT CASE WHEN autor.rol = 'Grupo' AND autor.idInvestigador != 0 THEN CONCAT(autor.firma, ' [', autor.idInvestigador, ']') END SEPARATOR ';') as 'Autores grupales US'",

    "COUNT(DISTINCT CASE WHEN autor.rol != 'Grupo' THEN autor.idAutor ELSE NULL END) as 'Total Autores'",
    "COUNT(DISTINCT CASE WHEN autor.rol = 'Grupo' THEN autor.idAutor ELSE NULL END) as 'Total Autores grupales'",

    "GROUP_CONCAT(DISTINCT CASE WHEN autor.rol != 'Grupo' THEN CONCAT(autor.idInvestigador, ',' , autor.orden, ',', autor.contacto) ELSE NULL END SEPARATOR ';') as 'lista_autores'",

    # Afiliaciones
    "GROUP_CONCAT(DISTINCT afiliacion.afiliacion SEPARATOR ';') AS 'Afiliaciones'",
    "CASE WHEN COUNT(DISTINCT CASE WHEN afiliacion.pais != 'Spain' THEN afiliacion.afiliacion ELSE NULL END) > 0 THEN 'Sí' ELSE 'No' END AS 'Afiliaciones Internacionales Sí/No'",
    "GROUP_CONCAT(DISTINCT CASE WHEN afiliacion.pais != 'Spain' THEN afiliacion.afiliacion ELSE NULL END SEPARATOR ';') AS 'Afiliaciones Internacionales'",

    # Identificadores
    "MAX(CASE WHEN idpub.tipo = 'doi' THEN idpub.valor ELSE NULL END) AS 'DOI'",
    "MAX(CASE WHEN idpub.tipo = 'wos' THEN idpub.valor ELSE NULL END) AS 'Cód. WOS'",
    "MAX(CASE WHEN idpub.tipo = 'scopus' THEN idpub.valor ELSE NULL END) AS 'Cód. Scopus'",
    "MAX(CASE WHEN idpub.tipo = 'pubmed' THEN idpub.valor ELSE NULL END) AS 'Cód. PubMed'",
    "MAX(CASE WHEN idpub.tipo = 'idus' THEN idpub.valor ELSE NULL END) AS 'Id. idUS'",
    "MAX(CASE WHEN idpub.tipo = 'dialnet' THEN idpub.valor ELSE NULL END) AS 'Cód. Dialnet'",

    # Acceso abierto
    "MAX(CASE WHEN acceso_abierto.origen = 'dialnet' THEN 'Texto completo' ELSE NULL END) AS 'Dialnet'",
    """    
    MAX(CASE WHEN acceso_abierto.origen = 'upw' THEN
        CASE
            WHEN acceso_abierto.valor = 'gold' THEN 'Dorada'
            WHEN acceso_abierto.valor = 'bronze' THEN 'Bronce'
            WHEN acceso_abierto.valor = 'hybrid' THEN 'Híbrida'
            WHEN acceso_abierto.valor = 'green' THEN 'Verde'
            WHEN acceso_abierto.valor = 'closed' THEN 'Sin Acceso Abierto'
            ELSE 'Sin Datos'
        END
    ELSE NULL END) AS 'Vía Acceso'""",

    # Citas
    "MAX(CASE WHEN citas.basedatos = 'wos' THEN citas.valor ELSE NULL END) AS 'Citas en WOS'",
    "MAX(CASE WHEN citas.basedatos = 'scopus' THEN citas.valor ELSE NULL END) AS 'Citas en Scopus'",
    "MAX(CASE WHEN citas.basedatos = 'dialnet' THEN citas.valor ELSE NULL END) AS 'Citas en Dialnet'",



]

# Joins para obtener todos los datos relacionados con cada publicación
joins = [
    # Calcular si una publicación tiene o no autoría grupal exclusiva
    # "LEFT JOIN (SELECT idPublicacion, MAX(idAutor) as idAutor FROM p_autor WHERE rol != 'Grupo' GROUP BY idPublicacion) autgrex ON p.idPublicacion = autgrex.idPublicacion",

    "LEFT JOIN p_fuente fuente ON fuente.idFuente = p.idFuente",
    "LEFT JOIN p_identificador_fuente idfuente ON fuente.idFuente = idfuente.idFuente",

    # Datos de publicación
    "LEFT JOIN p_dato_publicacion dp_volumen ON dp_volumen.idPublicacion = p.idPublicacion AND dp_volumen.tipo = 'volumen'",
    "LEFT JOIN p_dato_publicacion dp_numero ON dp_numero.idPublicacion = p.idPublicacion AND dp_numero.tipo = 'numero'",
    "LEFT JOIN p_dato_publicacion dp_pag_inicio ON dp_pag_inicio.idPublicacion = p.idPublicacion AND dp_pag_inicio.tipo = 'pag_inicio'",
    "LEFT JOIN p_dato_publicacion dp_pag_fin ON dp_pag_fin.idPublicacion = p.idPublicacion AND dp_pag_fin.tipo = 'pag_fin'",

    # Autores de cada publicación
    "LEFT JOIN p_autor autor ON autor.idPublicacion = p.idPublicacion",
    # Autores correspondientes al conjunto de autores del informe
    "LEFT JOIN (SELECT * FROM p_autor WHERE idInvestigador IN ({investigadores})) autor_inf ON autor_inf.idAutor = autor.idAutor",


    # Afiliaciones
    "LEFT JOIN p_autor_afiliacion autaf ON autaf.autor_id = autor.idAutor",
    "LEFT JOIN p_afiliacion afiliacion ON afiliacion.id = autaf.afiliacion_id",

    # Identificadores de publicación
    "LEFT JOIN p_identificador_publicacion idpub ON idpub.idPublicacion = p.idPublicacion",
    "LEFT JOIN p_acceso_abierto acceso_abierto ON acceso_abierto.publicacion_id = p.idPublicacion",

    # Métricas de publicación (citas)
    "LEFT JOIN m_publicaciones citas ON citas.idPublicacion = p.idPublicacion",
]


group_by = [
    "p.idPublicacion",
]

order_by = ["p.agno DESC",
            "p.idPublicacion"]


# Dado un conjunto de publicaciones e investigadores, devuelve datos asociados a esas publicaciones
# @timer
def datos_publicaciones(investigadores, publicaciones):

    _select = format_query(
        select, {"investigadores": investigadores, "publicaciones": publicaciones})
    _joins = format_query(
        joins, {"investigadores": investigadores, "publicaciones": publicaciones})

    query = f"SELECT {', '.join(_select)} FROM p_publicacion p"
    query += f" {' '.join(_joins)} "
    query += f" WHERE p.idPublicacion IN ({','.join(publicaciones)})"
    query += f" GROUP BY {','.join(group_by)}"
    query += f" ORDER BY {','.join(order_by)}"

    params = []

    db = BaseDatos()
    result = db.ejecutarConsulta(query, params)

    return result
