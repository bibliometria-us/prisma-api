class DatosCargaSJR:
    def __init__(
        self,
        id_fuente: int = None,
        journal: str = None,
        issn: str = None,
        issn_2: str = None,
        year: int = None,
        category: str = None,
        impact_factor: float = None,
        rank: str = None,
        quartile: str = None,
        decil: str = None,
        tercil: str = None,
    ):
        self.id_fuente = id_fuente
        self.journal = journal
        self.issn = issn
        self.issn_2 = issn_2
        self.year = int(year)
        self.category = category
        self.impact_factor = impact_factor
        self.rank = rank
        self.quartile = quartile
        self.decil = decil
        self.tercil = tercil

    def __eq__(self, value):
        if not isinstance(value, DatosCargaSJR):
            return False

        return (
            self.id_fuente == value.id_fuente
            and self.year == value.year
            and self.category == value.category
            and self.impact_factor == value.impact_factor
            and self.rank == value.rank
            and self.quartile == value.quartile
            and self.decil == value.decil
            and self.tercil == value.tercil
        )


class CategoriaSJR:
    def __init__(self, nombre: str):
        self.nombre = nombre
        self.datos: list[DatosCargaSJR] = []
        self.ranking = 0

    def insertar_dato(self, dato: DatosCargaSJR):
        self.ranking += 1
        self.datos.append(dato)
