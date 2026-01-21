from pandas import DataFrame
from routes.carga.financiacion.miembro import Miembro
import pandas as pd


def filtrar_componentes(componentes: DataFrame) -> DataFrame:
    # Reemplazar campos vacíos por None
    componentes = componentes.replace({"": None})

    # Convertir columnas de fecha a tipo fecha
    columnas_fecha = ["FechaAlta", "FechaBaja"]
    for columna_fecha in columnas_fecha:
        componentes[columna_fecha] = pd.to_datetime(
            componentes[columna_fecha], format="%d/%m/%Y", errors="coerce"
        ).dt.date

        componentes[columna_fecha] = componentes[columna_fecha].replace({pd.NaT: None})

    # Descartar componentes sin DNI
    componentes = componentes.dropna(subset=["NIF"])

    # Descartar componentes sin rol
    componentes = componentes.dropna(subset=["ParticipaComo"])

    return componentes


def cargar_componentes(componentes: DataFrame) -> list[str]:
    componentes = filtrar_componentes(componentes=componentes)

    result = []

    for componente in componentes.itertuples(index=True):
        result += cargar_componente(componente=componente)

    return result


def cargar_componente(componente) -> str:

    componente = Miembro(
        apellidos=componente.Apellidos,
        nombre=componente.Nombre,
        dni=componente.NIF,
        fecha_inicio=componente.FechaAlta,
        fecha_fin=componente.FechaBaja,
        fecha_renuncia=None,
        rol=componente.ParticipaComo,
        proyecto_id=componente.IdProyecto,
    )

    return componente.cargar()
