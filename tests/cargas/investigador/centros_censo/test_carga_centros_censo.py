from db.conexion import BaseDatos
import pandas as pd


from routes.carga.investigador.centros_censo.carga import carga_centros_censados
from tests.utils.random_utils import random_element


def test_carga_centros_censo(database: BaseDatos, seed):
    investigadores = seed["investigadores"]
    centros = seed["centros"]

    ruta_fichero, df = generar_csv(investigadores, centros)

    try:
        database.set_savepoint("carga_centros_censo")
        carga_centros_censados(ruta_fichero, db=database)

        for index, row in df.iterrows():
            dni = row["dni"]
            id_centro = row["id_centro"]

            comprobar_carga(dni, id_centro, database)

    finally:
        database.rollback_to_savepoint("carga_centros_censo")

    pass


def generar_csv(investigadores: dict[str, dict], centros: dict[str, dict]):
    df = pd.DataFrame(columns=["dni", "id_centro"])

    for investigador in investigadores.values():
        dni = investigador["docuIden"]

        centro: dict = random_element(centros.values())
        id_centro = centro["idCentro"]

        datos = {
            "dni": [dni],
            "id_centro": [id_centro],
        }

        row = pd.DataFrame(datos)
        df = pd.concat([df, row])

        ruta_fichero = "tests/temp/carga_centros_censo.csv"
        df.to_csv(ruta_fichero, index=False)

    return ruta_fichero, df


def comprobar_carga(dni: str, id_centro: str, db: BaseDatos):
    query = (
        "SELECT idCentroCenso FROM prisma.i_investigador WHERE docuIden = %(docuIden)s"
    )
    params = {"docuIden": dni}

    db.ejecutarConsulta(query, params)

    assert db.get_first_cell() == id_centro
