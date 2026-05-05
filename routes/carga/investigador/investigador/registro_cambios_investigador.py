from routes.carga.registro_cambios import RegistroCambios


class RegistroCambiosInvestigador(RegistroCambios):
    def __init__(
        self,
        id,
        tipo_dato,
        tipo_dato_2,
        tipo_dato_3,
        valor,
        valor_antiguo,
        origen,
        id_carga,
        bd=None,
    ):
        super().__init__(
            tabla="a_registro_cambios_investigador",
            id=id,
            tipo_dato=tipo_dato,
            tipo_dato_2=tipo_dato_2,
            tipo_dato_3=tipo_dato_3,
            origen=origen,
            valor=valor,
            valor_antiguo=valor_antiguo,
            autor="Unidad de Bibliometría",
            id_carga=id_carga,
            bd=bd,
        )


class RegistroCambiosNuevoInvestigador(RegistroCambiosInvestigador):
    def __init__(
        self,
        id,
        origen,
        id_investigador,
        id_carga,
        bd=None,
    ):
        super().__init__(
            id=id,
            tipo_dato="id_investigador",
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor=id_investigador,
            valor_antiguo=None,
            origen=origen,
            id_carga=id_carga,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = f"Investigador añadido ({self.origen})"


class RegistroCambiosInvestigadorAtributos(RegistroCambiosInvestigador):
    def __init__(
        self,
        id,
        atributo,
        valor,
        valor_antiguo,
        origen=None,
        bd=None,
        id_carga=None,
    ):
        super().__init__(
            id=id,
            tipo_dato=atributo,
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor=valor,
            valor_antiguo=valor_antiguo,
            origen=origen,
            bd=bd,
            id_carga=id_carga,
        )

    def generar_comentario(self):
        if self.tipo_dato == "docuIden":
            self.comentario = f"Documento de identidad modificado"
            return

        self.comentario = f"Actualizado {self.tipo_dato} de '{self.valor_antiguo or '<sin valor>'}' a '{self.valor}' ({self.origen})"


class RegistroCambiosInvestigadorCese(RegistroCambiosInvestigador):
    def __init__(
        self,
        id,
        origen,
        id_carga,
        bd=None,
    ):
        super().__init__(
            id=id,
            tipo_dato="cese",
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor="cesado",
            valor_antiguo=None,
            origen=origen,
            id_carga=id_carga,
            bd=bd,
        )

    def generar_comentario(self):
        self.comentario = "Investigador cesado"


class RegistroCambiosInvestigadorContrato(RegistroCambiosInvestigador):
    def __init__(
        self,
        id,
        origen,
        id_carga,
        tipo,
        bd=None,
    ):
        self.tipo = tipo
        super().__init__(
            id=id,
            tipo_dato="contrato",
            tipo_dato_2=None,
            tipo_dato_3=None,
            valor=None,
            valor_antiguo=None,
            origen=origen,
            id_carga=id_carga,
            bd=bd,
        )

    def generar_comentario(self):
        if self.tipo == "nuevo":
            self.comentario = "Investigador recontratado"
        elif self.tipo == "actualizado":
            self.comentario = "Investigador con contrato actualizado"
