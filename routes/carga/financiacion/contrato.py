from pandas import DataFrame, Timestamp
import pandas as pd

from db.conexion import BaseDatos
from routes.carga.financiacion.miembro import Miembro


class Contrato(Miembro):
    def __init__(
        self,
        organica: str,
        referencia: str,
        nombre: str,
        apellidos: str,
        dni: str,
        fecha_inicio: Timestamp,
        fecha_fin: Timestamp,
        fecha_renuncia: Timestamp,
    ):
        self.organica = organica
        self.referencia = referencia
        proyecto_id = self.buscar_id_proyecto()
        rol = "Contratado"
        super().__init__(
            proyecto_id,
            nombre,
            apellidos,
            rol,
            dni,
            fecha_inicio,
            fecha_fin,
            fecha_renuncia,
        )

    def buscar_id_proyecto(self):
        db = BaseDatos(database="prisma_proyectos")
        query = """
        SELECT id FROM proyecto 
        WHERE
            referencia = %(referencia)s
            OR
            REPLACE(referencia, '-', '') = %(referencia_sin_guion)s AND organica = %(organica)s
        """
        # Hay algunos contratos cuyos números de referencia no contienen guiones. Se busca respecto a la versión sin guiones y se busca la orgánica para verificar.
        params = {
            "referencia": self.referencia,
            "referencia_sin_guion": self.referencia.replace("-", ""),
            "organica": self.organica,
        }

        db.ejecutarConsulta(query, params=params)

        return db.get_first_cell()


def filtrar_contratos(contratos: DataFrame) -> DataFrame:
    # Reemplazar campos vacíos por None
    contratos = contratos.replace({"": None})

    # Convertir columnas de fecha a tipo fecha
    columnas_fecha = ["FechaInicio", "FechaFin", "FechaRenuncia"]
    for columna_fecha in columnas_fecha:
        contratos[columna_fecha] = pd.to_datetime(
            contratos[columna_fecha], format="%d/%m/%Y", errors="coerce"
        ).dt.date

        contratos[columna_fecha] = contratos[columna_fecha].replace({pd.NaT: None})

    # Descartar contratos sin fecha de inicio
    contratos = contratos.dropna(subset=["FechaInicio"])

    # Descartar contratos sin DNI
    contratos = contratos.dropna(subset=["NIF"])

    return contratos


def cargar_contratos(contratos: DataFrame) -> list[str]:
    contratos = filtrar_contratos(contratos=contratos)

    result = []

    for contrato in contratos.itertuples(index=True):
        result += cargar_contrato(contrato=contrato)

    return result


def cargar_contrato(contrato) -> list[str]:

    contrato = Contrato(
        organica=contrato.Organica,
        referencia=contrato.Referencia,
        apellidos=contrato.Apellidos,
        nombre=contrato.Nombre,
        dni=contrato.NIF,
        fecha_inicio=contrato.FechaInicio,
        fecha_fin=contrato.FechaFin,
        fecha_renuncia=contrato.FechaRenuncia,
    )

    return contrato.cargar()
