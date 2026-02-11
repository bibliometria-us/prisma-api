from datetime import datetime
import io
from pandas import DataFrame
import pandas as pd

from db.conexion import BaseDatos


def informe_at_revistas(año_metricas: int = None, año_at: int = None):
    año_actual = datetime.today().year

    año_at = año_at or año_actual
    año_metricas = año_metricas or año_actual - 2

    jif = informe_jif(año_jif=año_metricas, año_at=año_at)
    jci = informe_jci(año_jci=año_metricas, año_at=año_at)
    citescore = informe_citescore(año_citescore=año_metricas, año_at=año_at)

    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        jif.to_excel(writer, sheet_name="JIF", index=False)
        jci.to_excel(writer, sheet_name="JCI", index=False)
        citescore.to_excel(writer, sheet_name="Citescore", index=False)

    output.seek(0)

    return output


def informe_jif(año_jif: int, año_at: int) -> DataFrame:
    bd = BaseDatos()
    query = """
    SELECT
    ma.idFuente as 'ID Prisma',
    ma.titulo as 'Título',
    ma.editorial as 'Editorial',
    mj.edition as 'Edición',
    mj.category as 'Categoría',
    (SELECT MIN(quartile) FROM m_jcr WHERE 
                            idFuente = mj.idFuente 
                            AND year = mj.year
                            AND edition = mj.edition) as 'Cuartil máximo',
    mj.quartile as 'Cuartil',
    mj.decil as 'Decil',
    mj.tercil as 'Tercil'
    FROM m_at ma
    LEFT JOIN m_jcr mj ON mj.idFuente = ma.idFuente
    LEFT JOIN p_fuente pf ON pf.idFuente = ma.idFuente 
    WHERE ma.agno = %(año_at)s AND mj.year = %(año_jif)s
    GROUP BY mj.idFuente, mj.edition, mj.category
    """
    params = {"año_at": año_at, "año_jif": año_jif}

    bd.ejecutarConsulta(query, params=params)
    df = bd.get_dataframe()

    return df


def informe_jci(año_jci: int, año_at: int) -> DataFrame:
    bd = BaseDatos()
    query = """
    SELECT
    ma.idFuente as 'ID Prisma',
    ma.titulo as 'Título',
    ma.editorial as 'Editorial',
    mj.categoria as 'Categoría',
    (SELECT MIN(cuartil) FROM m_jci WHERE 
                            idFuente = mj.idFuente 
                            AND agno = mj.agno) as 'Cuartil máximo',
    mj.cuartil as 'Cuartil',
    mj.decil as 'Decil',
    mj.tercil as 'Tercil'
    FROM m_at ma
    LEFT JOIN m_jci mj ON mj.idFuente = ma.idFuente
    LEFT JOIN p_fuente pf ON pf.idFuente = ma.idFuente 
    WHERE ma.agno = %(año_at)s AND mj.agno = %(año_jci)s
    GROUP BY mj.idFuente, mj.categoria
    """
    params = {"año_at": año_at, "año_jci": año_jci}

    bd.ejecutarConsulta(query, params=params)
    df = bd.get_dataframe()

    return df


def informe_citescore(año_citescore: int, año_at: int) -> DataFrame:
    bd = BaseDatos()
    query = """
    SELECT
    ma.idFuente as 'ID Prisma',
    ma.titulo as 'Título',
    ma.editorial as 'Editorial',
    mj.categoria as 'Categoría',
    (SELECT MIN(cuartil) FROM m_jci WHERE 
                            idFuente = mj.idFuente 
                            AND agno = mj.agno) as 'Cuartil máximo',
    mj.cuartil as 'Cuartil',
    mj.decil as 'Decil',
    mj.tercil as 'Tercil'
    FROM m_at ma
    LEFT JOIN m_citescore mj ON mj.idFuente = ma.idFuente
    LEFT JOIN p_fuente pf ON pf.idFuente = ma.idFuente 
    WHERE ma.agno = %(año_at)s AND mj.agno = %(año_citescore)s
    GROUP BY mj.idFuente, mj.categoria
    """
    params = {"año_at": año_at, "año_citescore": año_citescore}

    bd.ejecutarConsulta(query, params=params)
    df = bd.get_dataframe()

    return df
