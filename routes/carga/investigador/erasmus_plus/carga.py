import pandas as pd
import os
from db.conexion import BaseDatos
from integration.email.email import enviar_correo
from logger.async_request import AsyncRequest
from celery import shared_task


@shared_task(
    queue="cargas", name="carga_erasmus_plus", ignore_result=True, acks_late=True
)
def task_carga_erasmus_plus(request_id: str):
    print(">>> Entrando en task_carga_erasmus_plus")
    request = AsyncRequest(id=request_id)
    ruta_fichero = request.params["ruta"]

    try:
        carga_erasmus_plus(ruta_fichero)
        request.close(message="Carga Erasmus+ realizada con éxito.")

        enviar_correo(
            # destinatarios=[request.email],
            # TODO: cambiar mail
            destinatarios="fmacias3@us.es",
            asunto="Carga Erasmus+ completada",
            texto_plano="",
            texto_html="La carga del fichero Erasmus+ ha finalizado correctamente.",
        )

    except Exception as e:
        request.error(message=str(e))

        enviar_correo(
            destinatarios=[request.email],
            asunto="Error en la carga Erasmus+",
            texto_plano="",
            texto_html=f" Ha ocurrido un error en la carga Erasmus+:<br>{str(e)}",
        )

    finally:
        os.remove(ruta_fichero)


def carga_erasmus_plus(ruta_fichero: str, db: BaseDatos = None):
    # Por defecto, BaseDatos se conecta a la base de datos prisma, por lo que hay que cambiarla a prisma_erasmus_plus
    # (tanto instancia como nueva conexión)
    if db is None:
        db = BaseDatos(database="prisma_erasmus_plus")
    else:
        # Si ya tiene conexión activa, la cerramos
        if db.is_active:
            db.closeConnection()
        db.database = "prisma_erasmus_plus"
        db.startConnection()
    xls = pd.ExcelFile(ruta_fichero)

    # Las cabeceras de las columnas pueden tener espacios en blanco al principio o al final
    df_proyectos = xls.parse("Proyectos")
    df_proyectos.columns = df_proyectos.columns.str.strip()

    df_participantes = xls.parse("Participantes")
    df_participantes.columns = df_participantes.columns.str.strip()

    df_instituciones = xls.parse("Instituciones")
    df_instituciones.columns = df_instituciones.columns.str.strip()

    procesar_proyectos(df_proyectos, db)
    procesar_participantes(df_participantes, db)
    procesar_instituciones(df_instituciones, db)


def procesar_proyectos(df: pd.DataFrame, db: BaseDatos):
    for index, row in df.iterrows():
        try:
            referencia = str(row["Referencia"]).strip()

            # 1. Consulta si ya existe
            consulta = "SELECT * FROM erasmus_proyectos WHERE referencia = %s"
            resultado = db.ejecutarConsulta(consulta, (referencia,))
            existe = db.rowcount > 0

            # 2. Construir diccionario de valores
            valores = {
                "denominacion": str(row["Denominación"]).strip(),
                "acronimo": str(row["Acrónimo"]).strip(),
                "fecha_inicio": str(row["Fecha inicio"]).strip(),
                "fecha_fin": str(row["Fecha Fin"]).strip(),
                "entidad_financiadora": str(row["Entidad financiadora:"]).strip(),
                "programa_financiador": str(row["Programa financiador:"]).strip(),
                "convocatoria": str(row["Convocatoria"]).strip(),
                "iniciativa": str(row["Iniciativa / Acción"]).strip(),
                "importe_total_concedido": str(row["Importe Total Concedido"]).strip(),
                "importe_asignado_us": str(row["Importe Asignado US"]).strip(),
                "referencia": referencia,
                "web": str(row["Web"]).strip(),
                "descripcion": str(row["Breve descripción / Objetivo"]).strip(),
                "convocatoria_competitiva": str(
                    row["Convocatoria competitiva (s/n)"]
                ).strip(),
                "innovacion_docente": str(
                    row["Proyecto de innovacion docente"]
                ).strip(),
            }

            if not existe:
                # 3. INSERT si no existe
                columnas = ", ".join(valores.keys())
                placeholders = ", ".join(["%s"] * len(valores))
                sql_insert = f"INSERT INTO erasmus_proyectos ({columnas}) VALUES ({placeholders})"
                db.ejecutarConsulta(sql_insert, list(valores.values()))

            else:
                # 4. Comparar si hay diferencias
                columnas_resultado = resultado[0]
                fila_resultado = dict(zip(columnas_resultado, resultado[1]))

                hay_cambios = any(
                    str(valores[k]) != str(fila_resultado.get(k, "")).strip()
                    for k in valores
                )

                if hay_cambios:
                    # 5. UPDATE si hay diferencias
                    set_clause = ", ".join(
                        [f"{k} = %s" for k in valores if k != "referencia"]
                    )
                    sql_update = f"""
                        UPDATE erasmus_proyectos
                        SET {set_clause}
                        WHERE referencia = %s
                    """
                    params = [valores[k] for k in valores if k != "referencia"] + [
                        referencia
                    ]
                    db.ejecutarConsulta(sql_update, params)
        except Exception as e:
            raise Exception(
                f"Error procesando fila {index+1} con referencia {row.get('Referencia', '')}: {str(e)}"
            )


def procesar_participantes(df: pd.DataFrame, db: BaseDatos):
    for index, row in df.iterrows():
        try:
            referencia = str(row["Referencia"]).strip()
            dni = str(row["DNI"]).strip()

            # 1. Consulta si ya existe
            sql_select = """
                SELECT * FROM erasmus_participantes
                WHERE referencia = %s AND dni = %s
            """
            resultado = db.ejecutarConsulta(sql_select, (referencia, dni))
            existe = db.rowcount > 0

            # 2. Diccionario de valores
            valores = {
                "referencia": referencia,
                "nombre": str(row["Nombre"]).strip(),
                "apellido": str(row["Apellido"]).strip(),
                "dni": dni,
                "rol": str(row["Rol"]).strip(),
            }

            if not existe:
                # 3. INSERT si no existe
                columnas = ", ".join(valores.keys())
                placeholders = ", ".join(["%s"] * len(valores))
                sql_insert = f"INSERT INTO erasmus_participantes ({columnas}) VALUES ({placeholders})"
                db.ejecutarConsulta(sql_insert, list(valores.values()))
            else:
                # 4. Comparar y UPDATE si hay diferencias
                columnas_resultado = resultado[0]
                fila_resultado = dict(zip(columnas_resultado, resultado[1]))

                hay_cambios = any(
                    str(valores[k]) != str(fila_resultado.get(k, "")).strip()
                    for k in valores
                )

                if hay_cambios:
                    set_clause = ", ".join(
                        [f"{k} = %s" for k in valores if k not in ("referencia", "dni")]
                    )
                    sql_update = f"""
                        UPDATE erasmus_participantes
                        SET {set_clause}
                        WHERE referencia = %s AND dni = %s
                    """
                    params = [
                        valores[k] for k in valores if k not in ("referencia", "dni")
                    ] + [referencia, dni]
                    db.ejecutarConsulta(sql_update, params)
        except Exception as e:
            raise Exception(
                f"Error procesando fila {index+1} con referencia {row.get('Referencia', '')}: {str(e)}"
            )


def procesar_instituciones(df: pd.DataFrame, db: BaseDatos):
    for index, row in df.iterrows():
        try:
            referencia = str(row["Referencia"]).strip()
            institucion = str(row["Instituciones"]).strip()
            pais = str(row["País"]).strip()
            rol = str(row["Rol"]).strip()

            # 1. Consulta si ya existe
            sql_select = """
                SELECT * FROM erasmus_instituciones
                WHERE referencia = %s AND institucion = %s AND pais = %s AND rol = %s
            """
            resultado = db.ejecutarConsulta(
                sql_select, (referencia, institucion, pais, rol)
            )
            existe = db.rowcount > 0

            # 2. Diccionario de valores
            valores = {
                "referencia": referencia,
                "institucion": institucion,
                "pais": pais,
                "rol": rol,
            }

            if not existe:
                # 3. INSERT si no existe
                columnas = ", ".join(valores.keys())
                placeholders = ", ".join(["%s"] * len(valores))
                sql_insert = f"INSERT INTO erasmus_instituciones ({columnas}) VALUES ({placeholders})"
                db.ejecutarConsulta(sql_insert, list(valores.values()))
            else:
                # 4. Comparar y UPDATE si hay diferencias
                columnas_resultado = resultado[0]
                fila_resultado = dict(zip(columnas_resultado, resultado[1]))

                hay_cambios = any(
                    str(valores[k]) != str(fila_resultado.get(k, "")).strip()
                    for k in valores
                )

                if hay_cambios:
                    set_clause = ", ".join(
                        [
                            f"{k} = %s"
                            for k in valores
                            if k not in ("referencia", "institucion", "pais", "rol")
                        ]
                    )
                    sql_update = f"""
                        UPDATE erasmus_instituciones
                        SET {set_clause}
                        WHERE referencia = %s AND institucion = %s AND pais = %s AND rol = %s
                    """
                    params = [
                        valores[k]
                        for k in valores
                        if k not in ("referencia", "institucion", "pais", "rol")
                    ] + [referencia, institucion, pais, rol]
                    db.ejecutarConsulta(sql_update, params)
        except Exception as e:
            raise Exception(
                f"Error procesando fila {index+1} con referencia {row.get('Referencia', '')}: {str(e)}"
            )
