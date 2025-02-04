from db.conexion import BaseDatos
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaAutor,
    DatosCargaDatoPublicacion,
    DatosCargaEditorial,
    DatosCargaFuente,
    DatosCargaIdentificadorFuente,
    DatosCargaIdentificadorPublicacion,
    DatosCargaPublicacion,
)


def busqueda_publicacion_por_id(id_publicacion, db: BaseDatos) -> DatosCargaPublicacion:
    # Buscar publicacion por id e introducir sus atributos
    datos_carga_publicacion = DatosCargaPublicacion()

    query_publicacion = (
        "SELECT * FROM prisma.p_publicacion WHERE idPublicacion = %(idPublicacion)s"
    )

    params_publicacion = {"idPublicacion": id_publicacion}

    db.ejecutarConsulta(query_publicacion, params_publicacion)
    publicacion = db.get_dataframe().iloc[0]

    datos_carga_publicacion.fuente_datos = publicacion["origen"]
    datos_carga_publicacion.titulo = publicacion["titulo"]
    datos_carga_publicacion.tipo = publicacion["tipo"]
    datos_carga_publicacion.año_publicacion = publicacion["agno"]

    buscar_autores(id_publicacion, db, datos_carga_publicacion)
    buscar_identificadores_publicacion(id_publicacion, db, datos_carga_publicacion)
    buscar_datos_publicacion(id_publicacion, db, datos_carga_publicacion)

    id_fuente = int(publicacion["idFuente"])
    buscar_fuente_publicacion(id_fuente, db, datos_carga_publicacion)
    buscar_editoriales_fuente(id_fuente, db, datos_carga_publicacion)
    buscar_identificadores_fuente(id_fuente, db, datos_carga_publicacion)

    return datos_carga_publicacion


def buscar_autores(
    id_publicacion, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):
    # Buscar e insertar autores
    lista_datos_autor: list[DatosCargaAutor] = []

    query_autores = (
        "SELECT * FROM prisma.p_autor WHERE idPublicacion = %(idPublicacion)s"
    )
    params_autores = {"idPublicacion": id_publicacion}

    db.ejecutarConsulta(query_autores, params_autores)
    autores = db.get_dataframe()

    for index, autor in autores.iterrows():
        dato_autor = DatosCargaAutor()

        dato_autor.firma = autor["firma"]
        dato_autor.tipo = autor["rol"]
        dato_autor.orden = autor["orden"]
        dato_autor.contacto = autor["contacto"]

        lista_datos_autor.append(dato_autor)

    datos_carga_publicacion.autores = lista_datos_autor


def buscar_identificadores_publicacion(
    id_publicacion, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):  # Buscar e insertar identificadores

    lista_datos_identificador_publicacion: list[DatosCargaIdentificadorPublicacion] = []

    query_identificadores_publicacion = "SELECT * FROM prisma.p_identificador_publicacion WHERE idPublicacion = %(idPublicacion)s"
    params_identificadores_publicacion = {"idPublicacion": id_publicacion}

    db.ejecutarConsulta(
        query_identificadores_publicacion, params_identificadores_publicacion
    )
    identificadores_publicacion = db.get_dataframe()

    for index, identificador_publicacion in identificadores_publicacion.iterrows():
        dato_identificador_publicacion = DatosCargaIdentificadorPublicacion()

        dato_identificador_publicacion.tipo = identificador_publicacion["tipo"]
        dato_identificador_publicacion.valor = identificador_publicacion["valor"]

        lista_datos_identificador_publicacion.append(dato_identificador_publicacion)

    datos_carga_publicacion.identificadores = lista_datos_identificador_publicacion


def buscar_datos_publicacion(
    id_publicacion, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):
    lista_datos_dato_publicacion: list[DatosCargaDatoPublicacion] = []

    query_datoes_publicacion = "SELECT * FROM prisma.p_dato_publicacion WHERE idPublicacion = %(idPublicacion)s"
    params_datoes_publicacion = {"idPublicacion": id_publicacion}

    db.ejecutarConsulta(query_datoes_publicacion, params_datoes_publicacion)
    datos_publicacion = db.get_dataframe()

    for index, dato_publicacion in datos_publicacion.iterrows():
        dato_dato_publicacion = DatosCargaDatoPublicacion()

        dato_dato_publicacion.tipo = dato_publicacion["tipo"]
        dato_dato_publicacion.valor = dato_publicacion["valor"]

        lista_datos_dato_publicacion.append(dato_dato_publicacion)

    datos_carga_publicacion.datos = lista_datos_dato_publicacion


def buscar_fuente_publicacion(
    id_fuente, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):
    # Buscar e insertar fuente

    datos_carga_fuente = DatosCargaFuente()
    datos_carga_fuente.id_fuente = int(id_fuente)

    query_fuente = "SELECT * FROM prisma.p_fuente WHERE idFuente = %(idFuente)s"
    params_query_fuente = {"idFuente": datos_carga_fuente.id_fuente}

    db.ejecutarConsulta(query_fuente, params_query_fuente)
    datos_fuente = db.get_dataframe().iloc[0]

    datos_carga_fuente.titulo = datos_fuente["titulo"]
    datos_carga_fuente.tipo = datos_fuente["tipo"]

    datos_carga_publicacion.fuente = datos_carga_fuente


def buscar_editoriales_fuente(
    id_fuente, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):
    # Buscar e insertar editoriales de fuente

    lista_datos_carga_editorial: list[DatosCargaEditorial] = []

    query_editoriales = """SELECT * FROM prisma.p_editor e
                            INNER JOIN (
                                SELECT valor FROM prisma.p_dato_fuente WHERE tipo = 'editorial' 
                                                                    AND idFuente = %(idFuente)s
                                        ) df ON df.valor = e.id
                        """
    params_query_editoriales = {"idFuente": id_fuente}

    db.ejecutarConsulta(query_editoriales, params_query_editoriales)
    editoriales = db.get_dataframe()

    if not editoriales.empty:
        for index, datos_editorial in editoriales.iterrows():
            datos_carga_editorial = DatosCargaEditorial()

            datos_carga_editorial.id_editor = datos_editorial["id"]
            datos_carga_editorial.nombre = datos_editorial["nombre"]
            datos_carga_editorial.tipo = datos_editorial["tipo"]
            datos_carga_editorial.pais = datos_editorial["pais"]
            datos_carga_editorial.url = datos_editorial["url"]

            lista_datos_carga_editorial.append(datos_carga_editorial)

    datos_carga_publicacion.fuente.editoriales = lista_datos_carga_editorial


def buscar_identificadores_fuente(
    id_fuente, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):
    # Buscar e insertar identificadores de fuente

    lista_datos_identificador_fuente: list[DatosCargaIdentificadorFuente] = []

    query_identificadores_fuente = (
        "SELECT * FROM prisma.p_identificador_fuente WHERE idFuente = %(idFuente)s"
    )
    params_query_identificadores_fuente = {"idFuente": id_fuente}

    db.ejecutarConsulta(
        query_identificadores_fuente, params_query_identificadores_fuente
    )
    identificadores_fuente = db.get_dataframe()

    if not identificadores_fuente.empty:
        for index, datos_identificador_fuente in identificadores_fuente.iterrows():
            datos_carga_identificador_fuente = DatosCargaIdentificadorFuente()

            datos_carga_identificador_fuente.tipo = datos_identificador_fuente["tipo"]
            datos_carga_identificador_fuente.valor = datos_identificador_fuente["valor"]

            lista_datos_identificador_fuente.append(datos_carga_identificador_fuente)

    datos_carga_publicacion.fuente.identificadores = lista_datos_identificador_fuente
