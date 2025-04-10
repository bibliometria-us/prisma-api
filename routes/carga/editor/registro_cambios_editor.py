from routes.carga.registro_cambios import RegistroCambios


class RegistroCambiosEditor(RegistroCambios):
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
            tabla="a_registro_cambios_editor",
            id=id,
            tipo_dato=tipo_dato,
            tipo_dato_2=tipo_dato_2,
            tipo_dato_3=tipo_dato_3,
            origen=origen,
            valor=valor,
            autor=autor,
            bd=bd,
        )


class RegistroCambiosEditorAtributos(RegistroCambiosEditor):
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
