from db.conexion import BaseDatos
from models.colectivo.rama import Rama


def test_create_get_rama(database: BaseDatos, seed: dict):
    database.rollback_to_savepoint("seed")
    seed_ramas = seed.get("ramas")
    for id, datos_rama in seed_ramas.items():
        rama = Rama(db=database)
        rama.get_primary_key().value = id
        rama.get()
        rama_dict = rama.to_dict()

        assert rama_dict == datos_rama
        assert rama_dict["nombre"]
