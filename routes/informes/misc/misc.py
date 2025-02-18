from db.conexion import BaseDatos


def get_metrica_calidad(bd: BaseDatos) -> dict:
    # Publicaciones con datos repetidos
    query_publicacion = """SELECT 
        p.idPublicacion AS ID, 
        pd.tipo AS PROBLEMA, 
        GROUP_CONCAT(DISTINCT pd.valor SEPARATOR "; ") AS VALORES_REPETIDOS,
        ib.nombre AS BIBLIOTECA
        FROM publicacionesXcentro p
        INNER JOIN p_dato_publicacion pd ON pd.idPublicacion = p.idPublicacion
        INNER JOIN i_centro ic ON ic.idCentro = p.idCentro
        INNER JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idCentro 
        WHERE p.eliminado = 0 AND pd.tipo != "titulo_alt"
        GROUP BY p.idPublicacion, pd.tipo, ib.nombre
        HAVING COUNT(DISTINCT pd.valor) > 1;"""

    try:
        bd.ejecutarConsulta(query_publicacion)
        metrica = bd.get_dataframe().set_index("ID").to_dict(orient="index")
    except Exception as e:
        return {"error": e.message}, 400

    return metrica
