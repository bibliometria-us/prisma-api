from werkzeug.datastructures import FileStorage
from db.conexion import BaseDatos
from utils.format import flask_csv_to_df


def cargar_programas():
    with open(
        "routes/carga/investigador/doctorado/fuentes/PROGRAMAS.csv", "rb"
    ) as file:
        file_storage = FileStorage(
            stream=file,
            name="PROGRAMAS.csv",
            content_type="application/octet-stream",
        )
        programas = flask_csv_to_df(file_storage)

    db = BaseDatos()

    for index, programa in programas.iterrows():
        query = """
                REPLACE INTO i_doctorado (idDoctorado, nombre, url)
                VALUES (%(idDoctorado)s, %(nombre)s, %(url)s)
                """

        params = {
            "idDoctorado": programa["CODIGO"],
            "nombre": programa["DOCTORADO"],
            "url": programa["WEB"],
        }

        db.ejecutarConsulta(query, params)


def cargar_lineas_investigacion():
    with open(
        "routes/carga/investigador/doctorado/fuentes/LINEAS_INVESTIGACION.csv", "rb"
    ) as file:
        file_storage = FileStorage(
            stream=file,
            name="LINEAS_INVESTIGACION.csv",
            content_type="application/octet-stream",
        )
        lineas_investigacion = flask_csv_to_df(file_storage)

    db = BaseDatos()

    for index, linea in lineas_investigacion.iterrows():

        query = """
                REPLACE INTO i_linea_investigacion_doctorado (idLineaInvestigacion, nombre)
                VALUES (%(idLineaInvestigacion)s, %(nombre)s);
                INSERT INTO i_doctorado_linea_investigacion (idDoctorado, idLineaInvestigacion)
                VALUES (%(idDoctorado)s, %(idLineaInvestigacion)s);
                """
        params = {
            "idDoctorado": linea["CODIGO"],
            "idLineaInvestigacion": linea["codigo linea"],
            "nombre": linea["LINEA INVESTIGACION"],
        }

        db.ejecutarConsulta(query, params)


def cargar_directores():
    with open(
        "routes/carga/investigador/doctorado/fuentes/DIRECTORES.csv", "rb"
    ) as file:
        file_storage = FileStorage(
            stream=file,
            name="DIRECTORES.csv",
            content_type="application/octet-stream",
        )
        directores = flask_csv_to_df(file_storage)

    db = BaseDatos()
    query = (
        "TRUNCATE TABLE i_profesor_doctorado;"
        "TRUNCATE TABLE i_profesor_doctorado_linea_inv;"
    )
    db.ejecutarConsulta(query)

    for index, director in directores.iterrows():

        query = """
                INSERT INTO i_profesor_doctorado (idInvestigador, idDoctorado)
                VALUES(
                    (SELECT idInvestigador FROM i_investigador WHERE docuIden = %(dni)s),
                    %(idDoctorado)s
                )
                """
        params = {
            "dni": director["C_DNI"],
            "idDoctorado": director["CODIGO"],
        }

        db.ejecutarConsulta(query, params)

        query = """
                INSERT INTO i_profesor_doctorado_linea_inv (idInvestigador, idLineaInvestigacion)
                VALUES(
                    (SELECT idInvestigador FROM i_investigador WHERE docuIden = %(dni)s),
                    %(idLineaInvestigacion)s
                )
                """
        params = {
            "dni": director["C_DNI"],
            "idLineaInvestigacion": director["LÍNEA DE INV."],
        }

        db.ejecutarConsulta(query, params)
