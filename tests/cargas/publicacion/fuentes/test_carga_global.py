import os
import pandas as pd
from db.conexion import BaseDatos
from tests.cargas.publicacion.fuentes.openalex.test_carga_openalex import carga_openalex
from tests.cargas.publicacion.fuentes.wos.test_carga_wos import carga_wos
from tests.cargas.publicacion.fuentes.scopus.test_carga_scopus import carga_scopus

LIMITE_PUBLICACIONES = 1000


def test_carga_global(database: BaseDatos):
    database.set_savepoint("carga_global_fuentes")
    try:
        carga_global(database)
        exportar_registro_y_problemas(database)
    finally:
        database.rollback_to_savepoint("carga_global_fuentes")


def carga_global(database: BaseDatos):
    limit = LIMITE_PUBLICACIONES
    carga_wos(database, limit=limit)
    carga_scopus(database, limit=limit)
    carga_openalex(database, limit=limit)


def exportar_registro_y_problemas(database: BaseDatos):
    query = """
        (SELECT 'Publicación' as "Tipo de documento", rcp.* FROM prisma.a_registro_cambios_publicacion rcp)
        UNION
        (SELECT 'Fuente' as "Tipo de documento", rcf.* FROM prisma.a_registro_cambios_fuente rcf)
        UNION
        (SELECT 'Editor' as "Tipo de documento", rce.* FROM prisma.a_registro_cambios_editor rce)
    """
    database.ejecutarConsulta(query)
    df_registros = database.get_dataframe()

    query = """
        (SELECT 'Publicación' as "Tipo de documento", rpp.* FROM prisma.a_registro_problemas_publicacion rpp)
        UNION
        (SELECT 'Fuente' as "Tipo de documento", rpf.* FROM prisma.a_registro_problemas_fuente rpf)
        UNION
        (SELECT 'Editor' as "Tipo de documento", rpe.* FROM prisma.a_registro_problemas_editor rpe)
    """
    database.ejecutarConsulta(query)
    df_problemas = database.get_dataframe()

    filename = "tests/temp/export_registros.xlsx"
    if os.path.exists(filename):
        os.remove(filename)
    with pd.ExcelWriter(filename) as writer:
        df_registros.to_excel(writer, sheet_name="Registros", index=False)
        df_problemas.to_excel(writer, sheet_name="Problemas", index=False)
