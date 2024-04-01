from time import sleep
from celery import shared_task, current_app, group
from db.conexion import BaseDatos
from integration.apis.clarivate.journals.exceptions import ExcepcionJournalWoS
from integration.apis.clarivate.journals.journals_api import JournalsAPI
from logger.logger import Log, LoggerMetadata
from utils.date import get_current_date


def iniciar_carga(fuentes: str, año_inicio: int, año_fin: int) -> str:
    lista_fuentes = obtener_lista_de_fuentes(fuentes)
    tasks = []
    current_date = get_current_date(format=True, format_str="%Y%m%d-%H%M%S-%f")

    for id_fuente in lista_fuentes:
        for año in range(año_inicio, año_fin + 1):
            tasks.append(
                current_app.tasks["actualizar_metrica_wos_journals"].s(
                    año, id_fuente, current_date
                )
            )

    LoggerMetadata("carga_wos_journal", current_date).start(len(tasks))

    group(tasks).apply_async()

    return "Carga iniciada con éxito"


def obtener_lista_de_fuentes(fuentes: str) -> list:
    db = BaseDatos()

    query_revistas = (
        "SELECT idFuente FROM p_fuente WHERE tipo IN ('Revista', 'Colección')"
    )
    filtrar_revistas = " AND idFuente IN ({})"
    query_order = " ORDER BY idFuente"

    if fuentes == "todas":
        pass
    else:
        lista_fuentes = list(f"{id_fuente}" for id_fuente in fuentes.split(","))
        filtrar_revistas = filtrar_revistas.format(",".join(lista_fuentes))
        query_revistas += filtrar_revistas

    query_revistas += query_order
    result = list(fila[0] for fila in db.ejecutarConsulta(query_revistas))[1:]

    return result


@shared_task(
    queue="wosjournals",
    name="actualizar_metrica_wos_journals",
    ignore_result=True,
    acks_late=True,
)
def carga_unitaria(año, id_fuente, fecha):
    db = BaseDatos()

    try:
        api = JournalsAPI(db, año, fecha, id_fuente)
        api.carga()
    except ExcepcionJournalWoS as e:
        pass
    except Exception as e:
        api.logger.add_log(
            log=Log(
                text=f"Error inesperado: {str(type(e).__name__)}. {e.args}",
                type="error",
            ),
            close=True,
        )
