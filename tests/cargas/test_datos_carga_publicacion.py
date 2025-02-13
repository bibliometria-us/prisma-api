from routes.carga.publicacion.datos_carga_publicacion import DatosCargaPublicacion
from tests.cargas.fuente import publicacion


def test_dict_parsing():
    source = publicacion
    datos_carga = DatosCargaPublicacion()
    datos_carga.from_dict(source=source)

    result_dict = datos_carga.to_dict()

    for key in source:
        assert source[key] == result_dict[key]

    nuevos_datos = DatosCargaPublicacion()
    nuevos_datos.from_dict(result_dict)

    assert nuevos_datos == datos_carga

    nuevo_dict = nuevos_datos.to_dict()

    assert nuevo_dict == source
