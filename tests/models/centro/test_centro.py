from db.conexion import BaseDatos
from models.colectivo.centro import Centro


def test_create_get_centro(database: BaseDatos, seed: dict):
    database.rollback_to_savepoint("seed")
    seed_centros = seed.get("centros")
    for id, datos_centro in seed_centros.items():
        centro = Centro(db=database)
        centro.get_primary_key().value = id
        centro.get()
        centro_dict = centro.to_dict()

        assert centro_dict == datos_centro
        assert centro_dict["nombre"]
