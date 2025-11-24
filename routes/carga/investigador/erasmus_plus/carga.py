import pandas as pd
from datetime import datetime
from db.conexion import BaseDatos
from integration.email.email import enviar_correo
from logger.async_request import AsyncRequest
from celery import shared_task
import os  # <-- añadido (se usa en os.remove)


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


def normalizar_valor(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "nat", "none"):
        return None
    return s


def normalizar_fecha(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "nat", "none"):
        return None
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


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

    # Mostrar número de filas en cada DataFrame
    print(
        "Filas en DataFrames: "
        f"proyectos={len(df_proyectos)}, "
        f"participantes={len(df_participantes)}, "
        f"instituciones={len(df_instituciones)}"
    )
    crear_respaldos_tablas(db)
    procesar_proyectos_sustituye(df_proyectos, db)
    procesar_participantes_sustituye(df_participantes, db)
    procesar_instituciones_sustituye(df_instituciones, db)


def crear_respaldos_tablas(db: BaseDatos):
    """Crea respaldos de las tablas con sufijo de fecha actual (YYYYMMDD)"""
    fecha_actual = datetime.now().strftime("%Y%m%d")
    tablas_respaldo = [
        ("erasmus_proyectos", f"erasmus_proyectos_{fecha_actual}"),
        ("erasmus_participantes", f"erasmus_participantes_{fecha_actual}"),
        ("erasmus_instituciones", f"erasmus_instituciones_{fecha_actual}"),
    ]

    for tabla_original, tabla_respaldo in tablas_respaldo:
        try:
            db.ejecutarConsulta(f"DROP TABLE IF EXISTS {tabla_respaldo}")
            db.ejecutarConsulta(f"CREATE TABLE {tabla_respaldo} LIKE {tabla_original}")
            db.ejecutarConsulta(
                f"INSERT INTO {tabla_respaldo} SELECT * FROM {tabla_original}"
            )
            print(f"Respaldo creado: {tabla_respaldo}")
        except Exception as e:
            print(f"Error creando respaldo de {tabla_original}: {e}")


def procesar_proyectos_sustituye(df: pd.DataFrame, db: BaseDatos):
    db.ejecutarConsulta("DELETE FROM erasmus_proyectos")
    # Forzar commit tras DELETE (crítico en producción)
    if hasattr(db, 'commit'):
        db.commit()
    try:
        db.ejecutarConsulta("ALTER TABLE erasmus_proyectos AUTO_INCREMENT = 1")
        if hasattr(db, 'commit'):
            db.commit()
    except Exception:
        pass
    errores = []
    for index, row in df.iterrows():
        try:
            referencia = normalizar_valor(row.get("Referencia"))
            valores = {
                "denominacion": normalizar_valor(row.get("Denominación")),
                "acronimo": normalizar_valor(row.get("Acrónimo")),
                "fecha_inicio": normalizar_fecha(row.get("Fecha inicio")),
                "fecha_fin": normalizar_fecha(row.get("Fecha Fin")),
                "entidad_financiadora": normalizar_valor(row.get("Entidad financiadora:")),
                "programa_financiador": normalizar_valor(row.get("Programa financiador:")),
                "convocatoria": normalizar_valor(row.get("Convocatoria")),
                "iniciativa": normalizar_valor(row.get("Iniciativa / Acción")),
                "importe_total_concedido": normalizar_valor(row.get("Importe Total Concedido")),
                "importe_asignado_us": normalizar_valor(row.get("Importe Asignado US")),
                "referencia": referencia,
                "web": normalizar_valor(row.get("Web")),
                "descripcion": normalizar_valor(row.get("Breve descripción / Objetivo")),
                "convocatoria_competitiva": normalizar_valor(row.get("Convocatoria competitiva (s/n)")),
                "innovacion_docente": normalizar_valor(row.get("Proyecto de innovacion docente")),
            }
            columnas = ", ".join(valores.keys())
            placeholders = ", ".join(["%s"] * len(valores))
            sql_insert = f"INSERT INTO erasmus_proyectos ({columnas}) VALUES ({placeholders})"
            db.ejecutarConsulta(sql_insert, list(valores.values()))
        except Exception as e:
            errores.append(f"fila {index+1} ref={row.get('Referencia','')}: {e}")
    # Commit final tras todos los INSERTs
    if hasattr(db, 'commit'):
        db.commit()
    if errores:
        print(f"[PROYECTOS] filas con error: {len(errores)}")
        for e in errores[:10]:
            print("  ", e)


def procesar_participantes_sustituye(df: pd.DataFrame, db: BaseDatos):
    db.ejecutarConsulta("DELETE FROM erasmus_participantes")
    if hasattr(db, 'commit'):
        db.commit()
    try:
        db.ejecutarConsulta("ALTER TABLE erasmus_participantes AUTO_INCREMENT = 1")
        if hasattr(db, 'commit'):
            db.commit()
    except Exception:
        pass
    errores = []
    for index, row in df.iterrows():
        try:
            referencia = normalizar_valor(row.get("Referencia"))
            dni = normalizar_valor(row.get("DNI"))
            valores = {
                "referencia": referencia,
                "nombre": normalizar_valor(row.get("Nombre")),
                "apellido": normalizar_valor(row.get("Apellido")),
                "dni": dni,
                "rol": normalizar_valor(row.get("Rol")),
            }
            columnas = ", ".join(valores.keys())
            placeholders = ", ".join(["%s"] * len(valores))
            sql_insert = f"INSERT INTO erasmus_participantes ({columnas}) VALUES ({placeholders})"
            db.ejecutarConsulta(sql_insert, list(valores.values()))
        except Exception as e:
            errores.append(f"fila {index+1} ref={row.get('Referencia','')} dni={row.get('DNI','')}: {e}")
    if hasattr(db, 'commit'):
        db.commit()
    if errores:
        print(f"[PARTICIPANTES] filas con error: {len(errores)}")
        for e in errores[:10]:
            print("  ", e)


def procesar_instituciones_sustituye(df: pd.DataFrame, db: BaseDatos):
    db.ejecutarConsulta("DELETE FROM erasmus_instituciones")
    if hasattr(db, 'commit'):
        db.commit()
    try:
        db.ejecutarConsulta("ALTER TABLE erasmus_instituciones AUTO_INCREMENT = 1")
        if hasattr(db, 'commit'):
            db.commit()
    except Exception:
        pass
    if "País" in df.columns:
        df["País"] = df["País"].astype(str).str.strip()
    errores = []
    for index, row in df.iterrows():
        try:
            referencia = normalizar_valor(row.get("Referencia"))
            institucion = normalizar_valor(row.get("Instituciones"))
            pais = normalizar_valor(row.get("País"))
            rol = normalizar_valor(row.get("Rol"))
            valores = {
                "referencia": referencia,
                "institucion": institucion,
                "pais": pais,
                "rol": rol,
            }
            columnas = ", ".join(valores.keys())
            placeholders = ", ".join(["%s"] * len(valores))
            sql_insert = f"INSERT INTO erasmus_instituciones ({columnas}) VALUES ({placeholders})"
            db.ejecutarConsulta(sql_insert, list(valores.values()))
        except Exception as e:
            errores.append(f"fila {index+1} ref={row.get('Referencia','')} inst={row.get('Instituciones','')}: {e}")
    if hasattr(db, 'commit'):
        db.commit()
    if errores:
        print(f"[INSTITUCIONES] filas con error: {len(errores)}")
        for e in errores[:10]:
            print("  ", e)
