from db.conexion import BaseDatos
from models.colectivo.categoria import Categoria


def test_create_get_categoria(database: BaseDatos, seed_categorias: dict):
    assert len(seed_categorias) > 1
    for id, datos_categoria in seed_categorias.items():
        categoria = Categoria(db=database)
        categoria.get_primary_key().value = id
        categoria.get()
        categoria_dict = categoria.to_dict()
        assert categoria_dict == datos_categoria
        assert categoria_dict["nombre"] is not None
