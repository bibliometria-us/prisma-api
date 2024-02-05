import math

def calcular_cuantil(percentil: float, tipo: str):
    tipo_a_division = {
        "tercil": 3.,
        "cuartil": 4.,
        "decil": 10.,
    }

    tipo_a_letra = {
        "tercil": "T",
        "cuartil": "Q",
        "decil": "D",
    }

    division = tipo_a_division[tipo]
    letra = tipo_a_letra[tipo]

    cuantil = str(math.ceil(division-percentil*(division/100)))

    result = letra + cuantil

    return result