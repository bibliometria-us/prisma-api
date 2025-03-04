import os
import time
from fandango.evolution.algorithm import Fandango, LoggerLevel
from fandango.language.parse import parse
import json

import pytest

from db.conexion import BaseDatos
from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.datos_carga_publicacion import DatosCargaPublicacion


def test_fuzzing_carga_publicacion(database: BaseDatos, seed: dict[str, dict]):
    lista_datos_carga_publicacion: list[DatosCargaPublicacion] = []

    with open(
        "tests/cargas/publicacion/fuzzing/input/datos_carga_publicacion.json", "r"
    ) as file:
        for line in file:
            datos_carga_publicacion = DatosCargaPublicacion()
            datos_carga_publicacion.from_json(line)
            lista_datos_carga_publicacion.append(datos_carga_publicacion)

    for datos_carga_publicacion in lista_datos_carga_publicacion:
        try:
            carga = CargaPublicacion(db=database)
            carga.datos = datos_carga_publicacion
            carga.origen = "idUS"

            carga.cargar_publicacion()

            carga.cargar_publicacion()

            assert datos_carga_publicacion == carga.datos_antiguos
        finally:
            database.rollback_to_savepoint("seed")


def generate_fuzzing():
    file = open("tests/cargas/publicacion/fuzzing/datos_carga_publicacion.fan", "r")

    grammar, constraints = parse(file, use_stdlib=False)
    solutions = []

    fandango = Fandango(
        grammar,
        constraints,
        desired_solutions=100000,
        logger_level=LoggerLevel.ERROR,
        best_effort=False,
    )
    fandango.evolve()
    solutions.extend(fandango.solution)

    output_file_path = (
        "tests/cargas/publicacion/fuzzing/input/datos_carga_publicacion.json"
    )

    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    with open(output_file_path, "w") as output_file:
        for solution in solutions:
            json_data = str(solution)
            output_file.write(json_data + "\n")


@pytest.mark.skip()
def test_generate_fuzzing():
    generate_fuzzing()
