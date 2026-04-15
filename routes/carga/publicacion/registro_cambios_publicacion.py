from routes.carga.registro_cambios import RegistroCambios


class RegistroCambiosPublicacion(RegistroCambios):
    def __init__(
        self,
        id,
        tipo_dato,
        tipo_dato_2,
        tipo_dato_3,
        valor,
        origen,
        autor,
        bd=None,
    ):
        super().__init__(
            tabla="a_registro_cambios_publicacion",
            id=id,
            tipo_dato=tipo_dato,
            tipo_dato_2=tipo_dato_2,
            tipo_dato_3=tipo_dato_3,
            origen=origen,
            valor=valor,
            autor=autor,
            bd=bd,
        )


class RegistroCambiosPublicacionAtributos(RegistroCambiosPublicacion):
    def __init__(self, id, atributo, valor, autor=None, origen=None, bd=None):
        super().__init__(
            id=id,
            tipo_dato=atributo,
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor=valor,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"{self.tipo_dato}: {self.valor} ({self.origen})"


class RegistroCambiosPublicacionDatos(RegistroCambiosPublicacion):
    def __init__(self, id, tipo_dato, valor, autor=None, origen=None, bd=None):
        super().__init__(
            id=id,
            tipo_dato="datos",
            tipo_dato_2=tipo_dato,
            tipo_dato_3=None,
            valor=valor,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"{self.tipo_dato_2}: {self.valor} ({self.origen})"


class RegistroCambiosPublicacionIdentificadores(RegistroCambiosPublicacion):
    def __init__(self, id, tipo_identificador, valor, autor=None, origen=None, bd=None):
        super().__init__(
            id=id,
            tipo_dato="identificadores",
            tipo_dato_2=tipo_identificador,
            tipo_dato_3=None,
            valor=valor,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"{self.tipo_dato_2}: {self.valor} ({self.origen})"


class RegistroCambiosPublicacionCantidadAutores(RegistroCambiosPublicacion):
    def __init__(self, id, valor, autor=None, origen=None, bd=None):
        super().__init__(
            id=id,
            tipo_dato="cantidad_autores",
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor=valor,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"{self.tipo_dato}: {self.valor} ({self.origen})"


class RegistroCambiosPublicacionFecha(RegistroCambiosPublicacion):
    def __init__(self, id, tipo_fecha, valor, autor=None, origen=None, bd=None):
        super().__init__(
            id=id,
            tipo_dato="fecha_publicacion",
            tipo_dato_2=tipo_fecha,
            tipo_dato_3=None,
            valor=valor,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"Fecha ({self.tipo_dato_2}): {self.valor} ({self.origen})"


class RegistroCambiosPublicacionOpenAccess(RegistroCambiosPublicacion):
    def __init__(
        self,
        id,
        tipo,
        valor,
        autor=None,
        origen=None,
        bd=None,
    ):
        super().__init__(
            id=id,
            tipo_dato="open_access",
            tipo_dato_2=tipo,
            tipo_dato_3=None,
            valor=valor,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"Open Access: {self.valor} ({self.origen})"


class RegistroCambiosPublicacionFinanciacion(RegistroCambiosPublicacion):
    def __init__(self, id, financiacion, autor=None, origen=None, bd=None):
        super().__init__(
            id=id,
            tipo_dato="financiacion",
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor=financiacion,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"{self.tipo_dato}: {self.valor} ({self.origen})"


class RegistroCambiosPublicacionFuente(RegistroCambiosPublicacion):
    def __init__(self, id, valor, autor=None, origen=None, bd=None):
        super().__init__(
            id=id,
            tipo_dato="id_fuente",
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor=valor,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"{self.tipo_dato}: {self.valor} ({self.origen})"
