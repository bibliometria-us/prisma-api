import pandas as pd
import os
from db.conexion import BaseDatos
from integration.email.email import enviar_correo
from logger.async_request import AsyncRequest
from celery import shared_task


@shared_task(
    queue="cargas", name="carga_centros_censo", ignore_result=True, acks_late=True
)
def task_carga_centros_censados(request_id: str):

    request = AsyncRequest(id=request_id)
    ruta_fichero = request.params["ruta"]

    try:
        carga_centros_censados(ruta_fichero)
        request.close(message="Carga realizada con éxito.")

        enviar_correo(
            destinatarios=[request.email],
            asunto="Carga de centros del censo",
            texto_plano="",
            texto_html="La carga de centros del censo ha finalizado correctamente.",
        )
    except Exception as e:
        request.error(message=str(e))

        enviar_correo(
            destinatarios=[request.email],
            asunto="Carga de centros del censo",
            texto_plano="",
            texto_html=f"""Ha ocurrido un error en la carga de centros del censo:
            {str(e)}""",
        )
    finally:
        os.remove(ruta_fichero)


def carga_centros_censados(ruta_fichero: str, db: BaseDatos = None):
    db = db or BaseDatos()
    df = pd.read_csv(ruta_fichero)
    # TODO: Implementar toda la carga de centros censados. Esta función debe recibir como input el fichero (o su ruta)

    for index, row in df.iterrows():
        query = """
            UPDATE prisma.i_investigador
            SET idCentroCenso = %(idCentroCenso)s
            WHERE docuIden = %(docuIden)s
                """
        params = {
            "docuIden": row["dni"],
            "idCentroCenso": row["id_centro"],
        }

        db.ejecutarConsulta(query, params)

    pass
