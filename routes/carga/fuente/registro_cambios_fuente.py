from routes.carga.registro_cambios import RegistroCambios


class RegistroCambiosFuente(RegistroCambios):
    def __init__(
        self,
        id,
        tipo_dato,
        tipo_dato_2,
        tipo_dato_3,
        valor,
        autor,
        origen,
        bd=None,
    ):
        super().__init__(
            tabla="a_registro_cambios_fuente",
            id=id,
            tipo_dato=tipo_dato,
            tipo_dato_2=tipo_dato_2,
            tipo_dato_3=tipo_dato_3,
            origen=origen,
            valor=valor,
            autor=autor,
            bd=bd,
        )


class RegistroCambiosFuenteAtributos(RegistroCambiosFuente):
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


class RegistroCambiosFuenteDatos(RegistroCambiosFuente):
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
        self.comentario = f"{self.tipo_dato}: {self.valor} ({self.origen})"


class RegistroCambiosFuenteIdentificadores(RegistroCambiosFuente):
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
        self.comentario = f"{self.tipo_dato}: {self.valor} ({self.origen})"


class RegistroCambiosFuenteEditorial(RegistroCambiosFuente):
    def __init__(self, id, id_editorial, autor=None, origen=None, bd=None):
        super().__init__(
            id=id,
            tipo_dato="id_editorial",
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor=id_editorial,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"{self.tipo_dato}: {self.valor} ({self.origen})"


class RegistroCambiosFuenteColeccion(RegistroCambiosFuente):
    def __init__(self, id, valor, autor=None, origen=None, bd=None):
        super().__init__(
            id=id,
            tipo_dato="id_coleccion",
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor=valor,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"{self.tipo_dato}: {self.valor} ({self.origen})"
