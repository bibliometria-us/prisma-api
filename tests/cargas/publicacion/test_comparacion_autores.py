import copy
from pandas import DataFrame
from routes.carga.publicacion.comparar_autores import ComparacionAutores
from tests.cargas.fuente_comparacion_autores import autores_prueba


def test_autores_iguales():
    antiguos_autores = DataFrame(autores_prueba.values())
    nuevos_autores = DataFrame(autores_prueba.values())

    comparacion_autores = ComparacionAutores(nuevos_autores, antiguos_autores)
    recuento_autores = comparacion_autores.comparar()

    assert recuento_autores[0] == recuento_autores[1]

    pass


def test_autores_diferentes():
    antiguos_autores = DataFrame(autores_prueba.values())

    nuevos_autores_prueba: dict = copy.deepcopy(autores_prueba)
    nuevos_autores_prueba[max(nuevos_autores_prueba) + 1] = nuevos_autores_prueba[
        max(nuevos_autores_prueba)
    ]

    nuevos_autores = DataFrame(nuevos_autores_prueba.values())

    comparacion_autores = ComparacionAutores(nuevos_autores, antiguos_autores)
    recuento_autores = comparacion_autores.comparar()

    assert recuento_autores[0] != recuento_autores[1]

    pass
