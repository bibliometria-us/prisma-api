from pandas import DataFrame
from routes.carga.publicacion.comparar_autores import ComparacionAutores
from tests.cargas.fuente_comparacion_autores import autores_prueba


def test_autores_iguales():
    antiguos_autores = DataFrame(autores_prueba.values())
    nuevos_autores = DataFrame(autores_prueba.values())

    comparacion_autores = ComparacionAutores(nuevos_autores, antiguos_autores)
    comparacion_autores.comparar()

    assert comparacion_autores.nuevos_autores.equals(
        comparacion_autores.antiguos_autores
    )

    pass
