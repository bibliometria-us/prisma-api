from pandas import DataFrame
from db.conexion import BaseDatos
from routes.carga.publicacion.buscar_firma_autor import BusquedaFirmaAutor


def test_buscar_firma_autor():
    autores_prisma = buscar_autores_prisma()

    total = 0
    exito = 0
    errores = 0
    min_score = 1

    for _, publicacion in autores_prisma.groupby("id_publicacion", sort=False):
        publicacion_original = publicacion
        lista_firmas = publicacion["firma_autor"].tolist()
        publicacion_prueba = publicacion_original.copy()
        # ensure column exists and set all values to null
        publicacion_prueba["id_investigador"] = None

        max_score = 0
        for index, autor in publicacion_prueba.iterrows():
            if not autor["nombre_completo"]:
                continue

            nombre_autor = autor["nombre_completo"]
            busqueda = BusquedaFirmaAutor()

            busqueda.find_author_in_paper(
                nombre_autor, lista_firmas, direction="backward"
            )
            match = busqueda.get_best_match(min_score=0.0)

            if match is None:
                continue

            firma = match["signature"]
            score = match["score"]

            match_condition = publicacion_original["firma_autor"] == firma
            busqueda_id_investigador = publicacion_original.loc[
                match_condition, "id_investigador"
            ]

            if busqueda_id_investigador.empty:
                continue

            if score > max_score:
                max_score = score

            id_investigador = busqueda_id_investigador.iloc[0]
            publicacion_prueba.at[index, "id_investigador"] = id_investigador

        total += 1

        if publicacion_original.equals(publicacion_prueba):
            exito += 1
            if max_score < min_score and max_score > 0:
                min_score = max_score
        else:
            errores += 1

    ratio_exito = exito / total if total > 0 else 0

    print(
        f"Total publicaciones: {total}, Éxitos: {exito}, Errores: {errores}, Ratio de éxito: {ratio_exito}. Puntuación mínima encontrada: {min_score:.2f}."
    )


def buscar_autores_prisma() -> DataFrame:
    bd = BaseDatos()
    query = """
        SELECT CONCAT(ii.apellidos, ', ', ii.nombre) AS nombre_completo,
        pa.firma AS firma_autor,
        pa.idPublicacion AS id_publicacion,
        ii.idInvestigador AS id_investigador
        FROM p_autor pa
        LEFT JOIN i_investigador ii ON pa.idInvestigador = ii.idInvestigador
        ORDER BY pa.idPublicacion
        """

    bd.ejecutarConsulta(query)
    df = bd.get_dataframe()
    return df
