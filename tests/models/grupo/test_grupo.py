from db.conexion import BaseDatos
from models.colectivo.grupo import Grupo


def test_create_get_grupo(database: BaseDatos, seed_grupos: dict):
    assert len(seed_grupos) > 1
    for id, datos_grupo in seed_grupos.items():
        grupo = Grupo(db=database)
        grupo.get_primary_key().value = id
        grupo.get()
        grupo_dict = grupo.to_dict()
        assert grupo_dict == datos_grupo
        assert grupo_dict["nombre"] is not None
