from db.conexion import BaseDatos
from models.colectivo.departamento import Departamento


def test_create_get_departamento(database: BaseDatos, seed: dict):
    database.rollback_to_savepoint("seed")
    seed_departamentos = seed.get("departamentos")
    assert len(seed_departamentos) > 1
    for id, datos_departamento in seed_departamentos.items():
        departamento = Departamento(db=database)
        departamento.get_primary_key().value = id
        departamento.get()
        departamento_dict = departamento.to_dict()

        assert departamento_dict == datos_departamento
        assert departamento_dict["nombre"] is not None
