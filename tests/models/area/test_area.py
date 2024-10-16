from db.conexion import BaseDatos
from models.colectivo.area import Area


def test_create_get_area(database: BaseDatos, seed: dict):
    database.rollback_to_savepoint("seed")
    seed_areas = seed.get("areas")
    for id, datos_area in seed_areas.items():
        area = Area(db=database)
        area.get_primary_key().value = id
        area.get()
        area_dict = area.to_dict()

        assert area_dict == datos_area
        assert area_dict["nombre"]
