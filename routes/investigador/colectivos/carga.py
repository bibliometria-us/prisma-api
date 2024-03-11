from typing import List
from models.colectivo.centro_mixto import CentroMixto
from models.colectivo.colectivo import Colectivo
from models.colectivo.instituto import Instituto
from models.colectivo.unidad_excelencia import UnidadExcelencia
from models.condition import Condition
from models.investigador import Investigador

from utils.format import table_to_pandas


def cargar_colectivos_investigadores(data: List[List[str]]):
    pd = table_to_pandas(data)
    for index, row in pd.iterrows():
        id_investigador: str = row["ID de PRISMA"]
        rol: str = row["ROL"]
        nombre_colectivo: str = row["Nombre colectivo"]
        tipo: str = row["Tipo"]

        if not id_investigador:
            continue

        rol_dict = {
            "Miembro Ordinario": "Miembro ordinario",
            "COLAB": "Colaborador",
            "ORD": "Miembro ordinario",
            "Responsable": "Responsable",
            "ASOC": "Asociado",
            "ASOCIADO": "Asociado",
            "Secretario": "Secretario",
            "Secretaria": "Secretario",
        }

        rol = rol_dict[rol.strip()]

        id_investigador = id_investigador.strip()
        if "/" in id_investigador:
            url = id_investigador.split("/")
            id_investigador = url[-1]

        tipo_dict = {
            "UD Excelencia US": "unidad_excelencia",
            "Instituto": "instituto",
            "Centro mixto": "centro_mixto",
        }

        tipo = tipo_dict[tipo.strip()]

        try:
            id_investigador = int(id_investigador)
        except Exception:
            continue

        investigador = Investigador()
        investigador.set_attribute("idInvestigador", int(id_investigador))
        investigador.get()

        conditions_colectivo = [
            Condition(query=f"nombre = '{nombre_colectivo}'"),
            Condition(query=f"acronimo = '{nombre_colectivo}'"),
        ]

        tipo_to_class = {
            "centro_mixto": CentroMixto,
            "instituto": Instituto,
            "unidad_excelencia": UnidadExcelencia,
        }
        colectivo: Colectivo = tipo_to_class[tipo]()

        colectivo.set_attribute("nombre", nombre_colectivo)
        colectivo.get(conditions=conditions_colectivo, logical_operator="OR")
        colectivo.update_colectivo_from_investigador(
            investigador.get_primary_key().value, rol
        )
