class LimitePalabrasClave(Exception):
    def __init__(self, max_palabras_clave: int):
        self.max_palabras_clave = max_palabras_clave
        self.message = (
            f"No se pueden introducir más de {self.max_palabras_clave} palabras clave"
        )

    def __str__(self):
        return self.message


class PalabraClaveDuplicada(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self.message = "Palabra clave duplicada"

    def __str__(self) -> str:
        return self.message


class LineaInvestigacionDuplicada(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self.message = "Linea de investigación duplicada"

    def __str__(self) -> str:
        return self.message
