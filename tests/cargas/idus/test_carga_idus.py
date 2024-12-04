import copy
import json
from db.conexion import BaseDatos
from routes.carga.publicacion.idus.carga import CargaPublicacionIdus

with open("tests/cargas/idus/data.json", "r") as f:
    data_list = json.load(f)


def test_carga_idus(database: BaseDatos):
    carga = CargaPublicacionIdus(db=database)

    # Probar carga en todas las publicaciones
    carga.cargar_publicaciones_por_dict(data_list)

    query_contar_publicaciones = "SELECT COUNT(*) FROM prisma.p_publicacion"

    database.ejecutarConsulta(query_contar_publicaciones)
    cantidad_publicaciones = database.get_first_cell()

    assert cantidad_publicaciones == len(data_list)

    database.rollback_to_savepoint("seed")


def test_repetir_carga_idus(database: BaseDatos):
    carga = CargaPublicacionIdus(db=database)

    carga.cargar_publicaciones_por_dict(data_list)

    query_contar_publicaciones = "SELECT COUNT(*) FROM prisma.p_publicacion"
    query_contar_problemas = "SELECT COUNT(*) FROM prisma.a_problemas"

    # Contar las publicaciones cargadas
    database.ejecutarConsulta(query_contar_publicaciones)
    cantidad_publicaciones = database.get_first_cell()

    # Repetir carga con los mismos datos

    carga.cargar_publicaciones_por_dict(data_list)

    database.ejecutarConsulta(query_contar_publicaciones)
    cantidad_publicaciones_2 = database.get_first_cell()

    # Comprobar que la cantidad de publicaciones no varía
    assert cantidad_publicaciones == cantidad_publicaciones_2

    # Contar los problemas generados
    database.ejecutarConsulta(query_contar_problemas)
    cantidad_problemas = database.get_first_cell()

    # Al no haber variaciones en los datos, no debe haber problemas generados
    assert cantidad_problemas == 0

    database.rollback_to_savepoint("seed")


def transformar_datos(data_list):
    nuevos_datos = copy.deepcopy(data_list)
    problemas_esperados = 0
    # Para cada publicación, modificar los datos para forzar errores
    for data in nuevos_datos:
        metadata: dict = data["metadata"]

        # Modificar datos primarios
        metadata["dc.title"][0]["value"] = "prueba"
        problemas_esperados += 1

        tipo = metadata["dc.type"][0]["value"]
        nuevo_tipo = "info:eu-repo/semantics/conferenceObject"

        if tipo != nuevo_tipo:
            metadata["dc.type"][0]["value"] = nuevo_tipo
            problemas_esperados += 1

        metadata["dc.date.issued"][0]["value"] = "0000"
        problemas_esperados += 1

        # Modificar autores

        # Modificar el nombre del primero autor, esto no debería hacer saltar ninguna advertencia
        metadata["dc.creator"][0]["value"] = "prueba"

        # Clonar el primer autor y colocarlo al último puesto. Varía la longitud, debería lanzar una advertencia
        cantidad_autores = len(metadata["dc.creator"])
        metadata["dc.creator"].append(metadata["dc.creator"][0])
        metadata["dc.creator"][cantidad_autores]["place"] = cantidad_autores
        problemas_esperados += 1

        # Modificar datos
        tipos_datos = [
            "dc.publication.volumen",
            "dc.publication.issue",
            "dc.publication.initialPage",
            "dc.publication.endPage",
        ]

        for tipo in tipos_datos:
            dato = metadata.get(tipo)
            if dato:
                valor = dato[0]
                valor["value"] = "prueba"
                problemas_esperados += 1

        # Modificar identificadores
        doi = metadata.get("dc.identifier.doi")
        if doi:
            doi[0]["value"] = "prueba"
            problemas_esperados += 1

    return nuevos_datos, problemas_esperados


def test_repetir_carga_idus_modificada(database: BaseDatos):
    carga = CargaPublicacionIdus(db=database)

    carga.cargar_publicaciones_por_dict(data_list)

    query_contar_publicaciones = "SELECT COUNT(*) FROM prisma.p_publicacion"
    query_contar_problemas = "SELECT COUNT(*) FROM prisma.a_problemas"

    # Contar las publicaciones cargadas
    database.ejecutarConsulta(query_contar_publicaciones)
    cantidad_publicaciones = database.get_first_cell()

    nuevos_datos, problemas_esperados = transformar_datos(data_list)

    # Repetir carga con los mismos datos
    carga.cargar_publicaciones_por_dict(nuevos_datos)

    database.ejecutarConsulta(query_contar_publicaciones)
    cantidad_publicaciones_2 = database.get_first_cell()

    # Comprobar que la cantidad de publicaciones no varía
    assert cantidad_publicaciones == cantidad_publicaciones_2

    # Contar los problemas generados
    database.ejecutarConsulta(query_contar_problemas)
    cantidad_problemas = database.get_first_cell()

    # Al no haber variaciones en los datos, no debe haber problemas generados
    assert cantidad_problemas == problemas_esperados

    database.rollback_to_savepoint("seed")
