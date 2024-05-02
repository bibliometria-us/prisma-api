from models.condition import Condition
from models.grupo import Grupo
from models.investigador import Investigador
from models.linea_investigacion import LineaInvestigacion
from models.palabra_clave import PalabraClave
from routes.carga.investigador.grupos.config import tablas
from flask import request
from db.conexion import BaseDatos
from celery import shared_task, current_app, group
from celery.utils.log import get_task_logger
import os

from utils.format import table_to_pandas

logger = get_task_logger(__name__)

db = BaseDatos(database=None)


def actualizar_tabla_sica(file_path: str):

    table_name = file_path.split("/")[-1].split(".")[0].lower()

    db = BaseDatos(database="sica2", local_infile=True, keep_connection_alive=True)
    db.ejecutarConsulta(f"TRUNCATE TABLE {table_name};")
    query = f"""
        LOAD DATA LOCAL INFILE %s
        INTO TABLE {table_name}
        CHARACTER SET UTF8
        COLUMNS TERMINATED BY ';'
        OPTIONALLY ENCLOSED BY '\"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES;
    """

    params = [file_path]
    result = db.ejecutarConsulta(query, params)
    db.closeConnection()
    os.remove(file_path)
    return result


def actualizar_grupos_sica():

    db = BaseDatos(database=None, keep_connection_alive=True)

    # Consulta de grupos a cargar
    query_lista_grupos = f"""
    SELECT CONCAT(MIN(g.RAMA), '-', MIN(g.CODIGO)) as idGrupo,
            MIN(g.NOMBRE) as nombre,
            MIN(g.ACRONIMO) as acronimo,
            MIN(g.RAMA) as rama,
            MIN(g.CODIGO) as codigo,
            MIN(i.ENTIDAD) as institucion,
            MIN(g.ESTADO) as estado,
            inv.idInvestigador as idInvestigador,
            CASE WHEN inv.idCategoria != "honor" THEN MIN(ig.rol) ELSE "Miembro" END as rol,
            ig.FECHA_FIN
    FROM prisma.i_investigador inv
    LEFT JOIN sica2.t_investigadores inv_s ON inv_s.NUMERO_DOCUMENTO = inv.docuIden
 
    LEFT JOIN (
        SELECT ID_PERSONAL, MAX(ID_GRUPO) as ID_GRUPO,
        (SELECT ROL FROM sica2.t_investigadores_grupo igprol
        WHERE igprol.ID_PERSONAL = igp.ID_PERSONAL
                AND igprol.ID_GRUPO = igp.ID_GRUPO
                AND (FECHA_FIN IS NULL OR FECHA_FIN = "" OR STR_TO_DATE(FECHA_FIN, '%d/%m/%Y') > now())
        ORDER BY CASE WHEN ROL = "Investigador principal" THEN 0
                    ELSE 1 END
        LIMIT 1        
            ) 
        as ROL,
        MAX(FECHA_FIN) as FECHA_FIN FROM sica2.t_investigadores_grupo igp
        
        
        WHERE FECHA_FIN IS NULL OR FECHA_FIN = "" OR STR_TO_DATE(FECHA_FIN, '%d/%m/%Y') > now()
        
            
        GROUP BY ID_PERSONAL, ID_GRUPO

       
        ) ig ON ig.ID_PERSONAL = inv_s.ID_PERSONAL
 
    LEFT JOIN sica2.t_grupos g ON g.ID_GRUPO = ig.ID_GRUPO
    LEFT JOIN sica2.t_instituciones i ON i.ID_ENTIDAD = g.ID_ENTIDAD
    GROUP BY inv.idInvestigador;
    """

    lista_grupos = table_to_pandas(db.ejecutarConsulta(query_lista_grupos))

    tasks_actualizar_grupo = []
    for index, grupo in lista_grupos.iterrows():
        tasks_actualizar_grupo.append(
            current_app.tasks["cargar_grupo_sica"].s(grupo.to_dict())
        )
    group(tasks_actualizar_grupo).apply_async()

    # Consulta de palabras clave a cargar y grupos asociados
    query_lista_palabras_clave = """
    SELECT gp.idGrupo as idGrupo,
            PALABRA_CLAVE as palabra_clave,
            DATE_FORMAT(MAX(STR_TO_DATE(g.FECHA_INICIO, "%d/%m/%Y")), '%Y-%m-%d') as fecha
    FROM sica2.`t_palabrasclave` pc

    LEFT JOIN sica2.t_grupos g ON g.ID_GRUPO = pc.ID_GRUPO
    LEFT JOIN prisma.i_grupo gp ON CONCAT(g.RAMA, "-", g.CODIGO) = gp.idGrupo

    GROUP BY pc.PALABRA_CLAVE, pc.ID_GRUPO

    HAVING MAX(
        CASE WHEN STR_TO_DATE(pc.FECHA_FIN, '%d/%m/%Y') = "0000-00-00" THEN "9999-01-01"
        ELSE STR_TO_DATE(pc.FECHA_FIN, '%d/%m/%Y') END
    ) > CURRENT_DATE
    """

    lista_palabras_clave = table_to_pandas(
        db.ejecutarConsulta(query_lista_palabras_clave)
    )

    tasks_palabras_clave = []

    for index, palabra_clave in lista_palabras_clave.iterrows():
        tasks_palabras_clave.append(
            current_app.tasks["cargar_palabra_clave"].s(palabra_clave.to_dict())
        )

    # group(tasks_palabras_clave).apply_async()

    query_lista_lineas_investigacion = """
    SELECT gp.idGrupo as idGrupo,
            LINEA_INVESTIGACION as linea_investigacion,
            DATE_FORMAT(MAX(STR_TO_DATE(g.FECHA_INICIO, "%d/%m/%Y")), '%Y-%m-%d') as fecha
    FROM sica2.`t_lineasinvestigacion` li

    LEFT JOIN sica2.t_grupos g ON g.ID_GRUPO = li.ID_GRUPO
    LEFT JOIN prisma.i_grupo gp ON CONCAT(g.RAMA, "-", g.CODIGO) = gp.idGrupo

    GROUP BY li.LINEA_INVESTIGACION, li.ID_GRUPO

    HAVING MAX(
        CASE WHEN STR_TO_DATE(li.FECHA_FIN, '%d/%m/%Y') = "0000-00-00" THEN "9999-01-01"
        ELSE STR_TO_DATE(li.FECHA_FIN, '%d/%m/%Y') END
    ) > CURRENT_DATE
    """

    lista_lineas_investigacion = table_to_pandas(
        db.ejecutarConsulta(query_lista_lineas_investigacion)
    )

    tasks_lineas_investigacion = []

    for index, linea_investigacion in lista_lineas_investigacion.iterrows():
        tasks_lineas_investigacion.append(
            current_app.tasks["cargar_linea_investigacion"].s(
                linea_investigacion.to_dict()
            )
        )

    # group(tasks_lineas_investigacion).apply_async()
    current_app.tasks["finalizar_carga_sica"].apply_async()

    db.closeConnection()


@shared_task(
    queue="cargas", name="cargar_grupo_sica", ignore_result=True, acks_late=True
)
def cargar_grupo_sica(datos_grupo: dict):

    grupo = Grupo()
    grupo.set_attributes(
        {
            "idGrupo": datos_grupo.get("idGrupo"),
            "nombre": datos_grupo.get("nombre"),
            "acronimo": datos_grupo.get("acronimo"),
            "rama": datos_grupo.get("rama"),
            "codigo": datos_grupo.get("codigo"),
            "institucion": datos_grupo.get("institucion"),
            "estado": datos_grupo.get("estado"),
        }
    )
    grupo.update()

    id_investigador = datos_grupo.get("idInvestigador")
    rol = datos_grupo.get("rol")

    if id_investigador:
        investigador = Investigador()
        investigador.set_attribute("idInvestigador", int(id_investigador))
        if datos_grupo.get("idGrupo"):
            investigador.get()
            investigador.actualizar_grupo(datos_grupo.get("idGrupo"), rol)
        else:
            investigador.eliminar_grupo()


@shared_task(
    queue="cargas", name="cargar_palabra_clave", ignore_result=True, acks_late=True
)
def cargar_palabra_clave(datos_palabras_clave: dict):
    palabra_clave = PalabraClave()
    palabra_clave.set_attribute(
        "nombre", datos_palabras_clave.get("palabra_clave").strip()
    )
    conditions = [
        Condition(query=f"nombre = '{palabra_clave.get_attribute_value('nombre')}'")
    ]
    palabra_clave.get(conditions=conditions)

    if not palabra_clave.get_attribute_value("idPalabraClave"):
        palabra_clave.create(attribute_filter=["idPalabraClave"])

    palabra_clave.get(conditions=conditions)

    grupo = Grupo()
    grupo.set_attribute("idGrupo", datos_palabras_clave.get("idGrupo"))
    grupo.get()
    grupo.add_palabra_clave(
        palabra_clave.get_attribute_value("idPalabraClave"),
        datos_palabras_clave.get("fecha"),
    )


@shared_task(
    queue="cargas",
    name="cargar_linea_investigacion",
    ignore_result=True,
    acks_late=True,
)
def cargar_linea_investigacion(datos_linea_investigacion: dict):
    linea_investigacion = LineaInvestigacion()
    linea_investigacion.set_attribute(
        "nombre", datos_linea_investigacion.get("linea_investigacion").strip()
    )
    conditions = [
        Condition(
            query=f"nombre = '{linea_investigacion.get_attribute_value('nombre')}'"
        )
    ]
    linea_investigacion.get(conditions=conditions)

    if not linea_investigacion.get_attribute_value("idLineaInvestigacion"):
        linea_investigacion.create(attribute_filter=["idLineaInvestigacion"])

    linea_investigacion.get(conditions=conditions)

    grupo = Grupo()
    grupo.set_attribute("idGrupo", datos_linea_investigacion.get("idGrupo"))
    grupo.get()
    grupo.add_linea_investigacion(
        linea_investigacion.get_attribute_value("idLineaInvestigacion"),
        datos_linea_investigacion.get("fecha"),
    )


@shared_task(
    queue="cargas", name="finalizar_carga_sica", ignore_result=True, acks_late=True
)
def finalizar_carga_sica():
    db = BaseDatos(database=None)
    actualizar_fecha = "UPDATE prisma.a_configuracion SET valor = DATE_FORMAT(NOW(), '%d/%m/%Y') WHERE variable = 'ACTUALIZACION_GRUPOS'"

    result = db.ejecutarConsulta(actualizar_fecha)
    db.closeConnection()

    return result
