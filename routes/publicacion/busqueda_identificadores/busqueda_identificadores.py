from celery import current_app, group, shared_task
from db.conexion import BaseDatos
from integration.apis.openalex.openalex import OpenalexAPI
from logger.logger import Log, LoggerMetadata, TaskLogger
from utils.date import get_current_date


# Busca publicaciones que tengan DOI pero les falte un tipo de identificador dado.
def buscar_publicaciones(inicio: int, fin: int, tipo: str) -> dict[str, str]:
    db = BaseDatos()

    consulta = """SELECT
                    	p.idPublicacion as id,
                    	ip_doi.valor as doi
                    FROM
                    	p_publicacion p
                    LEFT JOIN (SELECT * FROM p_identificador_publicacion WHERE tipo = 'doi') as ip_doi ON
                    	ip_doi.idPublicacion = p.idPublicacion
                    LEFT JOIN (SELECT * FROM p_identificador_publicacion WHERE tipo = %(tipo)s) as ip_busqueda ON
                    	ip_busqueda.idPublicacion = p.idPublicacion
                    WHERE p.agno BETWEEN %(inicio)s AND %(fin)s
                    GROUP BY
                    	p.idPublicacion
                    HAVING COUNT(ip_doi.idPublicacion) = 1 AND COUNT(ip_busqueda.idPublicacion) = 0
                ;"""

    params = {
        "tipo": tipo,
        "inicio": inicio,
        "fin": fin,
    }

    db.ejecutarConsulta(consulta, params=params)
    df = db.get_dataframe()

    publicaciones = df.set_index("id")["doi"].to_dict()

    if tipo == "openalex":
        return busqueda_ids_doi(publicaciones)


def busqueda_ids_doi(publicaciones: dict[str, str]):
    tasks = []
    current_date = get_current_date(format=True, format_str="%Y%m%d-%H%M%S-%f")

    for id_fuente, doi in publicaciones.items():
        tasks.append(
            current_app.tasks["buscar_openalex_id_por_doi"].s(
                id_fuente, doi, current_date
            )
        )

    LoggerMetadata("buscar_openalex_id_por_doi", current_date).start(len(tasks))

    group(tasks).apply_async()

    return "Carga iniciada"


def cargar_id_openalex(id_publicacion, id_openalex):
    db = BaseDatos()

    query = (
        "REPLACE INTO p_identificador_publicacion (idPublicacion, tipo, valor, origen)"
        "VALUES (%(idPublicacion)s, 'openalex', %(valor)s, 'Openalex')"
    )

    params = {"idPublicacion": id_publicacion, "valor": id_openalex}

    db.ejecutarConsulta(query, params=params)

    if db.error:
        return "error"
    if db.rowcount == 0:
        return "duplicado"

    return "cargado"


@shared_task(
    queue="openalex",
    name="buscar_openalex_id_por_doi",
    ignore_result=True,
    acks_late=True,
    rate_limit="5/s",
)
def carga_unitaria(id_publicacion, doi, current_date):
    db = BaseDatos()

    try:
        api = OpenalexAPI()

        logger = TaskLogger(
            task_id=f"{id_publicacion}",
            date=current_date,
            task_name="buscar_openalex_id_por_doi",
        )

        busqueda_doi: list[dict] = api.get_publicaciones_por_doi(id=doi)

        if len(busqueda_doi) == 0:
            logger.add_log(
                log=Log(
                    text=f"Publicación {id_publicacion}: No se ha encontrado esta publicación en OpenAlex",
                    type="info",
                ),
                close=True,
            )

            return None

        url_openalex: str = busqueda_doi[0].get("id")
        id_openalex = url_openalex.replace("https://openalex.org/", "")

        logger.add_log(
            log=Log(
                text=f"Publicación {id_publicacion} encontrada con ID {id_openalex}.",
                type="info",
            )
        )

        resultado = cargar_id_openalex(
            id_openalex=id_openalex, id_publicacion=id_publicacion
        )

        if resultado == "cargado":
            logger.add_log(
                log=Log(
                    text=f"ID cargado en la base de datos satisfactoriamente.",
                    type="info",
                ),
                close=True,
            )

        if resultado == "duplicado":
            logger.add_log(
                log=Log(
                    text=f"Este ID ya estaba cargado en la base de datos previamente.",
                    type="info",
                ),
                close=True,
            )

        if resultado == "error":
            logger.add_log(
                log=Log(
                    text=f"Error inesperado al introducir el identificador en la base de datos",
                    type="error",
                ),
                close=True,
            )

    except Exception as e:
        logger.add_log(
            log=Log(
                text=f"Publicación {id_publicacion}: Error inesperado: {str(type(e).__name__)}. {e.args}",
                type="error",
            ),
            close=True,
        )
