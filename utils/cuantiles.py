import math
from utils.utils import constrain_to_range


def calcular_cuantil(percentil: float, tipo: str, reducir=True):
    tipo_a_division = {
        "tercil": 3.0,
        "cuartil": 4.0,
        "decil": 10.0,
    }

    tipo_a_letra = {
        "tercil": "T",
        "cuartil": "Q",
        "decil": "D",
    }

    division = tipo_a_division[tipo]
    letra = tipo_a_letra[tipo]

    if reducir:
        percentil -= 0.0000001

    cuantil = math.ceil(division - percentil * (division / 100))

    cuantil = int(constrain_to_range(cuantil, 1, division))

    result = letra + str(cuantil)

    return result
