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

    # Limpiar idCentroCenso de todos los centros afectados en la carga
    centros = df["id_centro"].unique()

    for centro in centros:
        query = """
                UPDATE i_investigador
                SET idCentroCenso = NULL
                WHERE idCentroCenso = %(idCentroCenso)s
                """
        params = {"idCentroCenso": centro}

        # db.ejecutarConsulta(query, params)

    # Cargar los miembros de centro
    for index, row in df.iterrows():
        dni: str = row["dni"]
        dni = dni.replace("-", "").upper()
        id_centro = row["id_centro"]

        if id_centro:
            query = """
                UPDATE prisma.i_investigador
                SET idCentroCenso = %(idCentroCenso)s
                WHERE docuIden = %(docuIden)s
                    """
            params = {
                "docuIden": dni,
                "idCentroCenso": id_centro,
            }

            db.ejecutarConsulta(query, params)

    pass
