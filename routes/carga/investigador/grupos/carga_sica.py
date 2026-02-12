from datetime import datetime
import pandas as pd
from pandas import DataFrame
from config.local_config import email_bib, email_admin
from db.conexion import BaseDatos
from integration.email.email import enviar_correo
from routes.carga.investigador.grupos.exception import ErrorCargaSica


def carga_sica(files: dict[str, DataFrame]):

    grupos = files.get("t_grupos")
    instituciones = files.get("t_instituciones")
    investigadores = files.get("t_investigadores")
    investigadores_grupo = files.get("t_investigadores_grupo")

    investigadores_grupo = filtrar_investigadores_grupo(
        investigadores_grupo=investigadores_grupo
    )

    investigadores_grupo = join_investigadores(
        investigadores_grupo=investigadores_grupo, investigadores=investigadores
    )

    investigadores_grupo = buscar_ids_investigador(
        investigadores_grupo=investigadores_grupo
    )

    investigadores_grupo = join_grupos(
        investigadores_grupo=investigadores_grupo, grupos=grupos
    )

    investigadores_grupo = join_instituciones(
        investigadores_grupo=investigadores_grupo, instituciones=instituciones
    )

    asunto_email = "Carga de grupos de SICA"

    try:
        insertar_grupos(investigadores_grupo=investigadores_grupo)
        insertar_miembros_grupos(investigadores_grupo=investigadores_grupo)
        actualizar_fecha_insercion()
    except ErrorCargaSica as e:
        enviar_correo(
            adjuntos=[],
            asunto=asunto_email,
            destinatarios=email_bib + email_admin,
            texto_plano="",
            texto_html=e,
        )
        return None

    enviar_correo(
        adjuntos=[],
        asunto=asunto_email,
        destinatarios=email_bib,
        texto_plano="",
        texto_html="Carga de grupos de SICA finalizada satisfactoriamente",
    )


def filtrar_investigadores_grupo(investigadores_grupo: DataFrame) -> DataFrame:

    # Cambiar tipos a fecha
    columnas_fecha = ["FECHA_INICIO", "FECHA_FIN"]
    for columna in columnas_fecha:
        investigadores_grupo[columna] = pd.to_datetime(
            investigadores_grupo[columna], format="%d/%m/%Y", errors="coerce"
        )

        investigadores_grupo[columna] = investigadores_grupo[columna].replace(
            {pd.NaT: None}
        )
    # Eliminar filas con fechas de fin pasadas o fechas de inicio todavía no vigente
    today = pd.Timestamp.now().normalize()
    investigadores_grupo = investigadores_grupo[
        (investigadores_grupo["FECHA_FIN"] > today)
        | (investigadores_grupo["FECHA_FIN"].isna())
    ]
    investigadores_grupo = investigadores_grupo[
        investigadores_grupo["FECHA_INICIO"] <= today
    ]

    # Ordena los miembros por id, y luego por fecha de inicio.
    investigadores_grupo = investigadores_grupo.sort_values(
        by=["ID_PERSONAL", "FECHA_INICIO"], ascending=True
    )

    # Se eliminan duplicados dejando el último. De esta forma, nos quedamos siempre con la entrada más actualizada.
    investigadores_grupo = investigadores_grupo.drop_duplicates(
        subset=["ID_PERSONAL"], keep="last"
    )

    return investigadores_grupo


def join_investigadores(
    investigadores_grupo: DataFrame, investigadores: DataFrame
) -> DataFrame:
    # Se hace left join de investigadores sobre investigadores_grupo, de esta forma, para cada fila de investigadores_grupo se obtiene el DNI de la persona.
    investigadores_grupo = investigadores_grupo.merge(
        investigadores[["ID_PERSONAL", "NUMERO_DOCUMENTO"]],
        on="ID_PERSONAL",
        how="left",
    )

    return investigadores_grupo


def join_grupos(investigadores_grupo: DataFrame, grupos: DataFrame) -> DataFrame:
    # Se hace left join de grupo sobre investigadores_grupo para obtener la rama y código de cada grupo
    investigadores_grupo = investigadores_grupo.merge(
        grupos[
            [
                "ID_GRUPO",
                "RAMA",
                "CODIGO",
                "NOMBRE",
                "ACRONIMO",
                "FECHA_INICIO",
                "ESTADO",
                "ID_ENTIDAD",
            ]
        ].rename(
            columns={
                "RAMA": "RAMA_GRUPO",
                "CODIGO": "CODIGO_GRUPO",
                "NOMBRE": "NOMBRE_GRUPO",
                "ACRONIMO": "ACRONIMO_GRUPO",
                "FECHA_INICIO": "FECHA_CREACION_GRUPO",
                "ESTADO": "ESTADO_GRUPO",
            }
        ),
        on="ID_GRUPO",
        how="left",
    )

    # Se crea una nueva columna ID_GRUPO_PRISMA en el formato que se guardará en base de datos
    investigadores_grupo["ID_GRUPO_PRISMA"] = (
        investigadores_grupo["RAMA_GRUPO"]
        + "-"
        + investigadores_grupo["CODIGO_GRUPO"].astype(str)
    )

    # Transformar FECHA_CREACION_GRUPO a tipo fecha
    investigadores_grupo["FECHA_CREACION_GRUPO"] = pd.to_datetime(
        investigadores_grupo["FECHA_CREACION_GRUPO"], format="%d/%m/%Y", errors="coerce"
    )
    investigadores_grupo["FECHA_CREACION_GRUPO"] = investigadores_grupo[
        "FECHA_CREACION_GRUPO"
    ].replace({pd.NaT: None})

    return investigadores_grupo


def join_instituciones(
    investigadores_grupo: DataFrame, instituciones: DataFrame
) -> DataFrame:
    # Se hace left join de instituciones por ID_ENTIDAD
    investigadores_grupo = investigadores_grupo.merge(
        instituciones[["ID_ENTIDAD", "ENTIDAD"]].rename(
            columns={"ENTIDAD": "NOMBRE_ENTIDAD"}
        ),
        on="ID_ENTIDAD",
        how="left",
    )

    return investigadores_grupo


def buscar_ids_investigador(investigadores_grupo: DataFrame) -> DataFrame:
    # Normalizar DNI para que esté en mayúsculas
    investigadores_grupo["NUMERO_DOCUMENTO"] = investigadores_grupo[
        "NUMERO_DOCUMENTO"
    ].str.upper()

    dnis = investigadores_grupo["NUMERO_DOCUMENTO"].tolist()

    # Se buscan todos los investigadores con DNI en la lista de DNIs del fichero
    db = BaseDatos()
    placeholders = ", ".join(["%s"] * len(dnis))
    query = f"SELECT docuIden, idInvestigador from i_investigador_activo WHERE docuIden IN ({placeholders})"

    db.ejecutarConsulta(query, params=dnis)
    id_inves_dni = db.get_dataframe()

    # Se hace join para unir la id de investigador
    investigadores_grupo = investigadores_grupo.merge(
        id_inves_dni.rename(
            columns={
                "docuIden": "NUMERO_DOCUMENTO",
                "idInvestigador": "ID_INVESTIGADOR",
            }
        ),
        on="NUMERO_DOCUMENTO",
        how="left",
    )

    # Se eliminan filas sin id de investigador
    investigadores_grupo = investigadores_grupo.dropna(subset=["ID_INVESTIGADOR"])

    investigadores_grupo["ID_INVESTIGADOR"] = investigadores_grupo[
        "ID_INVESTIGADOR"
    ].astype(int)

    return investigadores_grupo


def insertar_grupos(investigadores_grupo: DataFrame):
    grupos = investigadores_grupo.drop_duplicates(subset=["ID_GRUPO_PRISMA"])

    db = BaseDatos(autocommit=False, keep_connection_alive=True)

    try:
        for grupo in grupos.itertuples():

            query = (
                "REPLACE INTO i_grupo (idGrupo, nombre, acronimo, rama, codigo, institucion, ambito, fecha_creacion, estado)"
                "VALUES (%(idGrupo)s, %(nombre)s, %(acronimo)s, %(rama)s, %(codigo)s, %(institucion)s, %(ambito)s, %(fecha_creacion)s, %(estado)s)"
            )
            params = {
                "idGrupo": grupo.ID_GRUPO_PRISMA,
                "nombre": grupo.NOMBRE_GRUPO,
                "acronimo": grupo.ACRONIMO_GRUPO,
                "rama": grupo.RAMA_GRUPO,
                "codigo": grupo.CODIGO_GRUPO,
                "institucion": grupo.NOMBRE_ENTIDAD,
                "ambito": "Andalucía",
                "fecha_creacion": grupo.FECHA_CREACION_GRUPO,
                "estado": grupo.ESTADO_GRUPO,
            }

            db.ejecutarConsulta(query, params=params)

        db.commit()
        db.closeConnection()

    except Exception:
        raise ErrorCargaSica("Error inesperado cargando grupos")


def insertar_miembros_grupos(investigadores_grupo: DataFrame):
    try:
        db = BaseDatos(autocommit=False, keep_connection_alive=True)
        truncate_query = "TRUNCATE TABLE i_grupo_investigador"

        db.ejecutarConsulta(truncate_query)

        query_insertar_miembro = (
            "INSERT INTO i_grupo_investigador (idInvestigador, idGrupo, rol)"
            "VALUES (%(idInvestigador)s, %(idGrupo)s, %(rol)s)"
        )

        for miembro in investigadores_grupo.itertuples():
            params = {
                "idInvestigador": miembro.ID_INVESTIGADOR,
                "idGrupo": miembro.ID_GRUPO_PRISMA,
                "rol": miembro.ROL,
            }

            db.ejecutarConsulta(query_insertar_miembro, params=params)

        db.commit()
        db.closeConnection()

    except Exception:
        raise ErrorCargaSica("Error inesperado cargando los miembros de grupo")


def actualizar_fecha_insercion():
    db = BaseDatos()

    query = """UPDATE a_configuracion
        SET valor = %(fecha)s
        WHERE variable = 'ACTUALIZACION_GRUPOS'"""

    params = {"fecha": datetime.now().date().strftime("%d/%m/%Y")}

    db.ejecutarConsulta(query, params=params)
