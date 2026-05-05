from abc import ABC, abstractmethod
from turtle import pd

from routes.carga.investigador.datos_carga_investigador import (
    DatosCargaCeseInvestigador,
    DatosCargaContratoInvestigador,
    DatosCargaInvestigador,
)


# TODO: LA clase parser debería ser una clase abstracta que implemente los métodos de la clase CargaInvestigador y quizás deba llamarse ParserInvestigador
class ParserInvestigador(ABC):
    def __init__(self, data: dict, tipo_fichero: str = "pdi") -> None:
        # Se definen los atributos de la clase
        self.tipo_fichero = tipo_fichero
        self.datos_carga_investigador = DatosCargaInvestigador()
        self.data: dict = data
        self.carga()  # Con los datos recuperados, se rellena el objeto de investigador

    @abstractmethod
    def set_fuente_datos(self):
        pass

    @abstractmethod
    def cargar_nombre(self):
        pass

    @abstractmethod
    def cargar_apellidos(self):
        pass

    @abstractmethod
    def cargar_documento_identidad(self):
        pass

    @abstractmethod
    def cargar_email(self):
        pass

    @abstractmethod
    def cargar_nacionalidad(self):
        pass

    @abstractmethod
    def cargar_sexo(self):
        pass

    @abstractmethod
    def cargar_fecha_nacimiento(self):
        pass

    @abstractmethod
    def cargar_contrato(self):
        pass

    def carga(self):
        self.set_fuente_datos()
        self.cargar_nombre()
        self.cargar_apellidos()
        self.cargar_documento_identidad()
        self.cargar_email()
        self.cargar_nacionalidad()
        self.cargar_sexo()
        self.cargar_fecha_nacimiento()
        self.cargar_contrato()
        self.datos_carga_investigador.close()


class ParserCese(ABC):
    def __init__(self, data: dict, tipo_fichero: str = "pdi") -> None:
        # Se definen los atributos de la clase
        self.tipo_fichero = tipo_fichero
        self.datos_carga_cese_investigador = DatosCargaCeseInvestigador()
        self.data: dict = data
        self.carga()  # Con los datos recuperados, se rellena el objeto de cese

    @abstractmethod
    def set_fuente_datos(self):
        pass

    @abstractmethod
    def set_documento_identidad(self):
        pass

    @abstractmethod
    def set_tipo(self):
        pass

    @abstractmethod
    def set_valor(self):
        pass

    @abstractmethod
    def set_fecha(self):
        pass

    def carga(self):
        self.set_fuente_datos()
        self.set_documento_identidad()
        self.set_tipo()
        self.set_valor()
        self.set_fecha()
        self.datos_carga_cese_investigador.close()
