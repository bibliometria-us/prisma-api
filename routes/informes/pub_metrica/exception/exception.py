class InformeSinInvestigadores(Exception):
    def __init__(
        self,
        message="El conjunto de investigadores del informe no contiene ningún investigador actualmente activo",
    ):
        self.message = message
        super().__init__(self.message)


class InformeSinPublicaciones(Exception):
    def __init__(self, año_inicio, año_fin):
        self.message = f"El conjunto de investigadores activos del informe no tiene publicaciones asociadas en el rango de años {año_inicio}-{año_fin}"
        super().__init__(self.message)
