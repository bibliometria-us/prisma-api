from abc import ABC, abstractmethod

from routes.carga.investigador.datos_carga_investigador import DatosCargaInvestigador


# TODO: LA clase parser debería ser una clase abstracta que implemente los métodos de la clase CargaInvestigador y quizás deba llamarse ParserInvestigador
class Parser(ABC):
    def __init__(self):
        self.datos_carga_investigador = DatosCargaInvestigador()

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
    def cargar_categoria(self):
        pass

    @abstractmethod
    def cargar_area(self):
        pass

    @abstractmethod
    def cargar_departamento(self):
        pass

    @abstractmethod
    def cargar_centro(self):
        pass

    @abstractmethod
    def cargar_centro_censo(self):
        pass

    @abstractmethod
    def cargar_nacionalidad(self):
        pass

    @abstractmethod
    def cargar_sexo(self):
        pass

    @abstractmethod
    def cargar_fecha_contratacion(self):
        pass

    @abstractmethod
    def cargar_fecha_nacimiento(self):
        pass

    @abstractmethod
    def cargar_fecha_nombramiento(self):
        pass

    @abstractmethod
    def cargar_fecha_cese(self):
        pass

    def carga(self):
        self.set_fuente_datos()
        self.cargar_nombre()
        self.cargar_apellidos()
        self.cargar_documento_identidad()
        self.cargar_email()
        self.cargar_categoria()
        self.cargar_area()
        self.cargar_departamento()
        self.cargar_centro()
        self.cargar_centro_censo()
        self.cargar_nacionalidad()
        self.cargar_sexo()
        self.cargar_fecha_contratacion()
        self.cargar_fecha_nacimiento()
        self.cargar_fecha_nombramiento()
        self.cargar_fecha_cese()
        self.datos_carga_investigador.close()
