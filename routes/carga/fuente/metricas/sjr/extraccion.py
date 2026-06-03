import re

from pandas import DataFrame, isna
from math import ceil, floor
from routes.carga.fuente.metricas.sjr.datos import CategoriaSJR, DatosCargaSJR


class ExtraccionSJR:
    def __init__(self):
        self.categorias: dict[str, CategoriaSJR] = {}

    def leer_datos(self, data: DataFrame, year: int):
        for _, row in data.iterrows():
            categorias = list(
                categoria.strip() for categoria in row["Categories"].split(";")
            )
            issns = list(
                f"{issn.strip()[:4]}-{issn.strip()[4:]}"
                for issn in row["Issn"].split(",")
            )

            for categoria in categorias:
                nombre_categoria = re.sub(r"\(Q\d+\)", "", categoria).strip()
                cuartil = re.search(r"\(Q\d+\)", categoria)
                impact_factor = (
                    row["SJR"].replace(",", ".")
                    if isinstance(row["SJR"], str)
                    else row["SJR"]
                )

                if isna(impact_factor) or not impact_factor:
                    continue

                dato = DatosCargaSJR(
                    journal=row["Title"],
                    issn=issns[0],
                    issn_2=issns[1] if len(issns) > 1 else None,
                    year=str(year),
                    category=nombre_categoria,
                    impact_factor=float(impact_factor),
                    quartile=(
                        cuartil.group(0).replace("(", "").replace(")", "")
                        if cuartil
                        else None
                    ),
                )
                self.insertar_dato(dato)

        self.calcular_percentiles()
        pass

    def insertar_dato(self, dato: DatosCargaSJR):
        categoria = self.categorias.get(dato.category, CategoriaSJR(dato.category))

        if dato.category not in self.categorias:
            self.categorias[dato.category] = categoria
        categoria.insertar_dato(dato)

    def calcular_percentiles(self):

        for categoria in self.categorias.values():
            categoria.datos.sort(key=lambda x: x.impact_factor, reverse=True)
            total_datos = len(categoria.datos)
            for i, dato in enumerate(categoria.datos):
                dato.rank = f"{i + 1}/{total_datos}"
                percentil = (i + 1) / total_datos * 100

                if dato.quartile:
                    dato.tercil = f"T{min(3, ceil(percentil / (100/3)))}"
                    dato.decil = f"D{min(10, ceil(percentil / 10))}"
