from typing import List
from models.colectivo.centro_mixto import CentroMixto
from models.colectivo.colectivo import Colectivo
from models.colectivo.instituto import Instituto
from models.colectivo.unidad_excelencia import UnidadExcelencia
from models.condition import Condition
from models.institucion import Institucion

from utils.format import table_to_pandas


def cargar_instituciones(data: List[List[str]]) -> None:
    pd = table_to_pandas(data)
    for index, row in pd.iterrows():
        nombre: str = row["Nombre colectivo"]
        tipo: str = row["tipo"]
        instituciones: str = row["Institucionescotitulares"]

        tipo_to_colectivo_class = {
            "Unidad de excelencia US": UnidadExcelencia,
            "Centro mixto": CentroMixto,
            "Instituto Universitario": Instituto,
        }

        colectivo: Colectivo = tipo_to_colectivo_class[tipo]()
        colectivo.set_attribute("nombre", nombre)
        conditions_colectivo = [
            Condition(query=f"nombre = '{colectivo.get_attribute_value('nombre')}'")
        ]
        colectivo.get(conditions=conditions_colectivo)

        for nombre_institucion in instituciones.split(";"):
            nombre_institucion = nombre_institucion.strip()
            institucion = Institucion()
            institucion.set_attribute("nombre", nombre_institucion)
            conditions_institucion = [
                Condition(
                    query=f"nombre = '{institucion.get_attribute_value('nombre')}'"
                )
            ]
            institucion.get(conditions=conditions_institucion)
            colectivo.add_institucion(institucion.get_primary_key().value)

    return None
