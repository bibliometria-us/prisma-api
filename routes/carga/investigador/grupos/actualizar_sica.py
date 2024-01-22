import tempfile
from routes.carga.investigador.grupos.config import tablas
from flask import request
from db.conexion import BaseDatos
from celery import shared_task, current_app, chain, group
from celery.utils.log import get_task_logger
import os

from utils.format import table_to_pandas

logger = get_task_logger(__name__)

db_carga = BaseDatos(database = None, keep_connection_alive= True)


@shared_task(queue='cargas', name='actualizar_tabla_sica', ignore_result=True)
def actualizar_tabla_sica(file_path: str):

    table_name = file_path.split("/")[-1].split(".")[0].lower()

    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        csv_data = file.read()
    
    with tempfile.NamedTemporaryFile(mode='w+', delete=True) as temp_file:
        temp_file.write(csv_data)
        temp_file_path = temp_file.name
        
    db = BaseDatos(database="sica2", local_infile = True)
    query = f"""
        LOAD DATA LOCAL INFILE %s
        INTO TABLE {table_name}
        CHARACTER SET UTF8
        COLUMNS TERMINATED BY ';'
        OPTIONALLY ENCLOSED BY '\"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES;
    """
    
    params = [temp_file_path]
    result = db.ejecutarConsulta(query, params)
    os.remove(file_path)
    return result

@shared_task(queue='cargas', name='actualizar_grupos_sica', ignore_result=True)
def actualizar_grupos_sica(self):

    db = BaseDatos(database = None)

    query_lista_grupos = f"""
    SELECT CONCAT(MIN(g.RAMA), '-', MIN(g.CODIGO)) as idGrupo,
            MIN(g.NOMBRE) as nombre,
            MIN(g.ACRONIMO) as acronimo,
            MIN(g.RAMA) as rama,
            MIN(g.CODIGO) as codigo,
            MIN(i.ENTIDAD) as institucion,
            MIN(g.ESTADO) as estado
    FROM sica2.t_grupos g
    LEFT JOIN sica2.t_instituciones i ON i.ID_ENTIDAD = g.ID_ENTIDAD
    GROUP BY g.ID_GRUPO;
    """
    query_lista_investigadores = f"""
    SELECT i.idInvestigador, i.idGrupo, CONCAT(i.nombre, " ", i.apellidos) as nombre FROM prisma.i_investigador i
    """

    lista_grupos = table_to_pandas(db.ejecutarConsulta(query_lista_grupos))
    lista_investigadores = table_to_pandas(db.ejecutarConsulta(query_lista_investigadores))

    tasks_actualizar_grupo = []
    for index, grupo in lista_grupos.iterrows():
        tasks_actualizar_grupo.append(current_app.tasks['actualizar_grupo_sica'].s(grupo.to_dict()))

    
    tasks_actualizar_investigador = []
    for index, investigador in lista_investigadores.iterrows():
        tasks_actualizar_investigador.append(current_app.tasks['actualizar_grupo_investigador'].s(investigador.to_dict()))

    group(tasks_actualizar_grupo).apply_async()
    group(tasks_actualizar_investigador).apply_async()
    current_app.tasks['finalizar_carga_sica'].apply_async()

@shared_task(queue='cargas', name='actualizar_grupo_sica', ignore_result=True)
def actualizar_grupo_sica(grupo: dict):
    id_grupo = grupo["idGrupo"]
    query_buscar_grupo = f"SELECT * FROM prisma.i_grupo WHERE idGrupo = '{id_grupo}'"
    query_actualizar_grupo = f"""REPLACE INTO prisma.i_grupo VALUES (
                                '{grupo.get("idGrupo")}',
                                '{grupo.get("nombre")}',
                                '{grupo.get("acronimo")}',
                                '{grupo.get("rama")}',
                                '{grupo.get("codigo")}',
                                '{grupo.get("institucion")}',
                                '{grupo.get("estado")}'
                                )
                                """
    
    antiguo_grupo = db_carga.ejecutarConsulta(query_buscar_grupo)
    
    log = None
    nombre_grupo = f"{grupo.get('nombre')} ({grupo.get('idGrupo')})"

    db_carga.ejecutarConsulta(query_actualizar_grupo)

    if len(antiguo_grupo) == 1:
        log = f"Grupo insertado: {nombre_grupo}"
        return log

    pandas_antiguo_grupo = table_to_pandas(antiguo_grupo)
    dict_antiguo_grupo = pandas_antiguo_grupo.to_dict("index")[0]
    
    if dict_antiguo_grupo.get("estado") == "Válido" and grupo.get("estado") == "No Válido":
        log = f"Grupo {nombre_grupo} actualizado como no válido"

    if dict_antiguo_grupo.get("estado") == "No Válido" and grupo.get("estado") == "Válido":
        log = f"Grupo {nombre_grupo} actualizado como válido"

    return log

@shared_task(queue='cargas', name='actualizar_grupo_investigador', ignore_result=True)
def actualizar_grupo_investigador(investigador):
    query_buscar_grupo = """
                    SELECT
                    CASE WHEN pg.idGrupo IS NOT NULL THEN pg.idGrupo ELSE '0' END as idGrupo
                    FROM (SELECT * FROM prisma.i_investigador) i
                    LEFT JOIN sica2.t_investigadores si ON si.NUMERO_DOCUMENTO = i.docuIden
                    LEFT JOIN (SELECT MAX(ID_GRUPO) ID_GRUPO, ID_PERSONAL, FECHA_FIN FROM sica2.t_investigadores_grupo
                        WHERE FECHA_FIN = ''
                        GROUP BY ID_PERSONAL ) ig ON ig.ID_PERSONAL = si.ID_PERSONAL
                    LEFT JOIN sica2.t_grupos g ON g.ID_GRUPO = ig.ID_GRUPO
                    LEFT JOIN prisma.i_grupo pg ON pg.idGrupo =  CONCAT(g.RAMA, '-', g.CODIGO)
                    WHERE i.idInvestigador = %s
            """
    query_actualizar_grupo = """UPDATE prisma.i_investigador i
                                SET i.idGrupo = %s
                                WHERE i.idInvestigador = %s
                            """
    
    params = [investigador["idInvestigador"]]

    nuevo_grupo_investigador = table_to_pandas(db_carga.ejecutarConsulta(query_buscar_grupo, params)).to_dict("index")[0]

    antiguo_grupo = investigador["idGrupo"]
    nuevo_grupo = nuevo_grupo_investigador["idGrupo"]

    log = None

    if antiguo_grupo != nuevo_grupo:

        if nuevo_grupo == "0":
            log = f"{investigador['nombre']} ({investigador['idInvestigador']}) ha abandonado el grupo {antiguo_grupo}"
        if antiguo_grupo == "0":
            log = f"{investigador['nombre']} ({investigador['idInvestigador']}) es nuevo miembro del grupo {nuevo_grupo}"
        else:
            log = f"{investigador['nombre']} ({investigador['idInvestigador']}) ha cambiado su grupo de {antiguo_grupo} a {nuevo_grupo}"


        resultado = db_carga.ejecutarConsulta(query_actualizar_grupo, params = [nuevo_grupo, investigador['idInvestigador']])

    return log

@shared_task(queue='cargas', name='finalizar_carga_sica', ignore_result=True)
def finalizar_carga_sica():

    actualizar_fecha = "UPDATE prisma.a_configuracion SET valor = DATE_FORMAT(NOW(), '%d/%m/%Y') WHERE variable = 'ACTUALIZACION_GRUPOS'"
    
    result = db_carga.ejecutarConsulta(actualizar_fecha)
    db_carga.closeConnection()

    return result