from models.investigador import Investigador


def test_create(database, seed_investigadores: dict):
    assert len(seed_investigadores) > 1
    for id, datos_investigador in seed_investigadores.items():
        investigador = Investigador(db=database)
        investigador.get_primary_key().value = id
        investigador.get()
        investigador.components["area"].value.get()
        investigador.components["area"].value.components["rama"].value.get()
        investigador_dict = investigador.to_dict(depth=2)

        assert investigador_dict == datos_investigador
        assert investigador_dict["nombre"] is not None
