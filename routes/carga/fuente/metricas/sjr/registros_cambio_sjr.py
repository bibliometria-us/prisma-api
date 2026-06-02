from routes.carga.fuente.registro_cambios_fuente import RegistroCambiosFuente


class RegistroCambiosSJR(RegistroCambiosFuente):
    def __init__(
        self, id, atributo, year, valor, valor_antiguo, autor=None, origen=None, bd=None
    ):
        super().__init__(
            id=id,
            tipo_dato="sjr",
            tipo_dato_2=year,
            tipo_dato_3=atributo,
            valor=valor,
            valor_antiguo=valor_antiguo,
            origen=origen,
            autor=autor,
            bd=bd,
        )

    def generar_comentario(self):
        if self.valor_antiguo:
            self.comentario = f"{self.tipo_dato.upper()} {self.tipo_dato_2} ({self.tipo_dato_2}): Actualizado de {self.valor_antiguo} a {self.valor}"
        else:
            self.comentario = f"{self.tipo_dato.upper()} {self.tipo_dato_2} ({self.tipo_dato_2}): Nuevo valor {self.valor}"
