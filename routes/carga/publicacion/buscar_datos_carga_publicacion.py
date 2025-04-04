import copy
from db.conexion import BaseDatos
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaAccesoAbierto,
    DatosCargaAfiliacionesAutor,
    DatosCargaAutor,
    DatosCargaDatoPublicacion,
    DatosCargaEditorial,
    DatosCargaFechaPublicacion,
    DatosCargaFinanciacion,
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
    df = db.get_dataframe()

    if df.empty:
        return datos_carga_publicacion

    publicacion = df.iloc[0]

    datos_carga_publicacion.fuente_datos = publicacion["origen"]
    datos_carga_publicacion.titulo = publicacion["titulo"]
    datos_carga_publicacion.tipo = publicacion["tipo"]
    datos_carga_publicacion.año_publicacion = publicacion["agno"]

    buscar_autores(id_publicacion, db, datos_carga_publicacion)
    buscar_afiliaciones_autor(id_publicacion, db, datos_carga_publicacion)
    buscar_identificadores_publicacion(id_publicacion, db, datos_carga_publicacion)
    buscar_datos_publicacion(id_publicacion, db, datos_carga_publicacion)

    id_fuente = int(publicacion["idFuente"])
    datos_carga_fuente = datos_carga_publicacion.fuente

    buscar_fuente_publicacion(id_fuente, db, datos_carga_fuente)
    buscar_editoriales_fuente(id_fuente, db, datos_carga_fuente)
    buscar_datos_fuente(id_fuente, db, datos_carga_fuente)
    buscar_identificadores_fuente(id_fuente, db, datos_carga_fuente)

    id_coleccion = buscar_coleccion_fuente(id_fuente, db)
    datos_carga_coleccion = datos_carga_publicacion.fuente.coleccion
    if id_coleccion is not None:
        datos_carga_fuente.coleccion.id_fuente = id_coleccion
        buscar_fuente_publicacion(id_coleccion, db, datos_carga_coleccion)
        buscar_editoriales_fuente(id_coleccion, db, datos_carga_coleccion)
        buscar_datos_fuente(id_coleccion, db, datos_carga_coleccion)
        buscar_identificadores_fuente(id_coleccion, db, datos_carga_coleccion)

    buscar_financiacion(id_publicacion, db, datos_carga_publicacion)
    buscar_fechas_publicacion(id_publicacion, db, datos_carga_publicacion)
    buscar_acceso_abierto(id_publicacion, db, datos_carga_publicacion)

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

        dato_autor.id_autor = autor["idAutor"]
        dato_autor.firma = autor["firma"]
        dato_autor.tipo = autor["rol"]
        dato_autor.orden = autor["orden"]
        dato_autor.contacto = autor["contacto"]

        lista_datos_autor.append(dato_autor)

    datos_carga_publicacion.autores = lista_datos_autor


def buscar_afiliaciones_autor(
    id_publicacion, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):
    for autor in datos_carga_publicacion.autores:
        query_afiliaciones_autor = """SELECT * FROM prisma.p_afiliacion a
            LEFT JOIN prisma.p_autor_afiliacion aa ON a.id = aa.afiliacion_id
            WHERE aa.autor_id = %(idAutor)s
            """
        params_afiliaciones_autor = {"idAutor": autor.id_autor}

        db.ejecutarConsulta(query_afiliaciones_autor, params_afiliaciones_autor)
        afiliaciones_autor = db.get_dataframe()

        lista_afiliaciones_autor: list[DatosCargaAfiliacionesAutor] = []
        for index, afiliacion_autor in afiliaciones_autor.iterrows():
            datos_afiliacion_autor = DatosCargaAfiliacionesAutor()

            datos_afiliacion_autor.nombre = afiliacion_autor["afiliacion"]
            datos_afiliacion_autor.pais = afiliacion_autor["pais"]
            datos_afiliacion_autor.ror_id = afiliacion_autor["id_ror"]

            lista_afiliaciones_autor.append(datos_afiliacion_autor)

        autor.afiliaciones = lista_afiliaciones_autor


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
    id_fuente,
    db: BaseDatos,
    datos_carga_fuente: DatosCargaFuente,
):
    # Buscar e insertar fuente

    datos_carga_fuente.id_fuente = int(id_fuente)

    query_fuente = "SELECT * FROM prisma.p_fuente WHERE idFuente = %(idFuente)s"
    params_query_fuente = {"idFuente": datos_carga_fuente.id_fuente}

    db.ejecutarConsulta(query_fuente, params_query_fuente)

    df = db.get_dataframe()

    if df.empty:
        return None

    datos_fuente = df.iloc[0]

    datos_carga_fuente.titulo = datos_fuente["titulo"]
    datos_carga_fuente.tipo = datos_fuente["tipo"]


def buscar_datos_fuente(id_fuente, db: BaseDatos, datos_carga_fuente: DatosCargaFuente):
    # Buscar e insertar datos de fuente

    lista_datos_dato_fuente: list[DatosCargaDatoPublicacion] = []

    query_datos_fuente = "SELECT * FROM prisma.p_dato_fuente WHERE idFuente = %(idFuente)s AND tipo IN ('url', 'titulo_alt')"
    params_datos_fuente = {"idFuente": id_fuente}

    db.ejecutarConsulta(query_datos_fuente, params_datos_fuente)
    datos_fuente = db.get_dataframe()

    if not datos_fuente.empty:
        for index, dato_fuente in datos_fuente.iterrows():
            datos_carga_dato_fuente = DatosCargaDatoPublicacion()

            datos_carga_dato_fuente.tipo = dato_fuente["tipo"]
            datos_carga_dato_fuente.valor = dato_fuente["valor"]

            lista_datos_dato_fuente.append(datos_carga_dato_fuente)

    datos_carga_fuente.datos = lista_datos_dato_fuente


def buscar_editoriales_fuente(
    id_fuente, db: BaseDatos, datos_carga_fuente: DatosCargaFuente
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

    datos_carga_fuente.editoriales = lista_datos_carga_editorial


def buscar_identificadores_fuente(
    id_fuente, db: BaseDatos, datos_carga_fuente: DatosCargaFuente
):
    # Buscar e insertar identificadores de fuente

    lista_datos_identificador_fuente: list[DatosCargaIdentificadorFuente] = []

    query_identificadores_fuente = """SELECT * FROM prisma.p_identificador_fuente idf WHERE idf.idFuente = %(idFuente)s
        GROUP BY idf.valor, idf.tipo
        """
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

    datos_carga_fuente.identificadores = lista_datos_identificador_fuente


def buscar_coleccion_fuente(id_fuente, db: BaseDatos):
    # Buscar e insertar coleccion de fuente

    query_coleccion = "SELECT valor FROM prisma.p_dato_fuente WHERE idFuente = %(idFuente)s AND tipo = 'coleccion'"
    params_query_coleccion = {"idFuente": id_fuente}

    db.ejecutarConsulta(query_coleccion, params_query_coleccion)
    return db.get_first_cell()


def buscar_fechas_publicacion(
    id_publicacion, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):
    # Buscar e insertar fechas de publicación

    query_fechas_publicacion = "SELECT tipo, mes, agno FROM prisma.p_fecha_publicacion WHERE idPublicacion = %(idPublicacion)s"
    params_fechas_publicacion = {"idPublicacion": id_publicacion}

    db.ejecutarConsulta(query_fechas_publicacion, params_fechas_publicacion)
    fechas_publicacion = db.get_dataframe()

    if fechas_publicacion.empty:
        datos_carga_publicacion.fechas_publicacion = []
        return None

    for index, fecha_publicacion in fechas_publicacion.iterrows():
        fecha = DatosCargaFechaPublicacion()

        fecha.tipo = fecha_publicacion["tipo"]
        fecha.mes = fecha_publicacion["mes"]
        fecha.agno = fecha_publicacion["agno"]

        datos_carga_publicacion.fechas_publicacion.append(fecha)


def buscar_acceso_abierto(
    id_publicacion, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):
    # Buscar e insertar acceso abierto

    query_acceso_abierto = "SELECT valor, origen FROM prisma.p_acceso_abierto WHERE publicacion_id = %(publicacion_id)s"
    params_acceso_abierto = {"publicacion_id": id_publicacion}

    db.ejecutarConsulta(query_acceso_abierto, params_acceso_abierto)
    acceso_abierto = db.get_dataframe()

    if acceso_abierto.empty:
        datos_carga_publicacion.acceso_abierto = []
        return None

    for index, oa in acceso_abierto.iterrows():
        datos_oa = DatosCargaAccesoAbierto()

        datos_oa.valor = oa["valor"]
        datos_oa.origen = oa["origen"]

        datos_carga_publicacion.acceso_abierto.append(datos_oa)


def buscar_financiacion(
    id_publicacion, db: BaseDatos, datos_carga_publicacion: DatosCargaPublicacion
):
    # Buscar e insertar financiación

    query_financiacion = "SELECT codigo, agencia FROM prisma.p_financiacion WHERE publicacion_id = %(publicacion_id)s"
    params_financiacion = {"publicacion_id": id_publicacion}

    db.ejecutarConsulta(query_financiacion, params_financiacion)
    financiacion = db.get_dataframe()

    if financiacion.empty:
        datos_carga_publicacion.financiacion = []
        return None

    lista_financiacion = []
    for index, fin in financiacion.iterrows():
        datos_financiacion = DatosCargaFinanciacion()

        datos_financiacion.proyecto = fin["codigo"]
        datos_financiacion.agencia = fin["agencia"]

        lista_financiacion.append(datos_financiacion)

    datos_carga_publicacion.financiacion = lista_financiacion
