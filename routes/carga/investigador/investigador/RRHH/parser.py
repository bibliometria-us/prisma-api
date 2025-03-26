from routes.carga.investigador.datos_carga_investigador import (
    DatosCargaInvestigador,
    DatosCargaCategoriaInvestigador,
    DatosCargaAreaInvestigador,
    DatosCargaDepartamentoInvestigador,
    DatosCargaCentroInvestigador,
    DatosCargaCentroCensoInvestigador,
    DatosCargaCeseInvestigador,
    DatosCargaContratoInvestigador,
)
from routes.carga.investigador.parser import Parser
from datetime import datetime
import pandas as pd


# *****************************
# **** PARSER INVESTIGADOR ****
# *****************************
class ParserInvestigador:
    def __init__(self, data: dict, tipo_fichero: str = "pdi") -> None:
        # Se definen los atributos de la clase
        self.tipo_fichero = tipo_fichero
        self.datos_carga_investigador = DatosCargaInvestigador()
        self.data: dict = data
        self.carga()  # Con los datos recuperados, se rellena el objeto de investigador

    def set_fuente_datos(self):
        if self.tipo_fichero == "pdi":
            self.datos_carga_investigador.set_fuente_datos("RRHH-PDI")
        else:
            self.datos_carga_investigador.set_fuente_datos("RRHH-PI")

    def cargar_nombre(self):
        self.datos_carga_investigador.set_nombre(self.data.get("NOMBRE_LEGAL").title())

    def cargar_apellidos(self):
        apellidos1 = self.data.get("APELLIDO1", None)
        apellidos2 = self.data.get("APELLIDO2", None)
        if apellidos2 and not pd.isna(apellidos2):
            apellidos = (
                self.data.get("APELLIDO1").title()
                + " "
                + self.data.get("APELLIDO2").title()
            )
        else:
            apellidos = self.data.get("APELLIDO1").title()
        self.datos_carga_investigador.set_apellidos(apellidos)

    def cargar_documento_identidad(self):
        self.datos_carga_investigador.set_documento_identidad(
            self.data.get("NIF").replace("-", "")
        )

    def cargar_email(self):
        self.datos_carga_investigador.set_email(self.data.get("CORREO_ELECTRÓNICO"))

    def cargar_nacionalidad(self):
        nacionalidad = self.data.get("NACIONALIDAD")
        if nacionalidad and not pd.isna(nacionalidad):
            self.datos_carga_investigador.set_nacionalidad(nacionalidad.title())
        else:
            self.datos_carga_investigador.set_nacionalidad("")

    def cargar_sexo(self):
        sexo = self.data.get("SEXO")
        sexo_bd = "3"
        match sexo:
            case "V":
                sexo_bd = "1"
            case "M":
                sexo_bd = "0"
        self.datos_carga_investigador.set_sexo(sexo_bd)

    def cargar_fecha_nacimiento(self):
        self.datos_carga_investigador.set_fecha_nacimiento(
            self.data.get("F_NACIMIENTO").strftime("%d/%m/%Y")
        )

    def cargar_contrato(self):
        contrato = DatosCargaContratoInvestigador()
        contrato.set_fecha_contratacion(self.data.get("F_INICIO").strftime("%d/%m/%Y"))
        contrato.set_fecha_nombramiento(
            self.data.get("F_NOMBRAMIENTO").strftime("%d/%m/%Y")
        )
        fecha_fin = self.data.get("F_FIN")
        if not pd.isna(fecha_fin):
            contrato.set_fecha_fin_contratacion(fecha_fin.strftime("%d/%m/%Y"))
        else:
            contrato.set_fecha_fin_contratacion("")
        contrato.set_centro(
            DatosCargaCentroInvestigador(
                id=self.data.get("CENTRO_DESTINO"),
                nombre=self.data.get("DES_CENTRO_DESTINO"),
            )
        )
        contrato.set_categoria(
            DatosCargaCategoriaInvestigador(
                id=self.data.get("CCE"), nombre=self.data.get("DES_CCE")
            )
        )
        contrato.set_departamento(
            DatosCargaDepartamentoInvestigador(
                id=self.data.get("DEPARTAMENTO"),
                nombre=self.data.get("DES_DEPARTAMENTO"),
            )
        )
        id_area = self.data.get("AREA")
        nombre_area = self.data.get("DES_AREA")
        area = DatosCargaAreaInvestigador(id="", nombre="")
        if not pd.isna(id_area) and not pd.isna(nombre_area):
            area = DatosCargaAreaInvestigador(
                id=self.data.get("AREA"), nombre=self.data.get("DES_AREA")
            )
        contrato.set_area(area)

        # CENTRO CENSO --> pass
        self.datos_carga_investigador.add_contrato(contrato)

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


# *****************************
# ******** PARSER CESE ********
# *****************************
class ParserCese:
    def __init__(self, data: dict, tipo_fichero: str = "pdi") -> None:
        # Se definen los atributos de la clase
        self.tipo_fichero = tipo_fichero
        self.datos_carga_cese_investigador = DatosCargaCeseInvestigador()
        self.data: dict = data
        self.carga()  # Con los datos recuperados, se rellena el objeto de cese

    def set_fuente_datos(self):
        if self.tipo_fichero == "pdi":
            self.datos_carga_cese_investigador.set_fuente_datos("RRHH-PDI")
        else:
            self.datos_carga_cese_investigador.set_fuente_datos("RRHH-PI")

    def set_documento_identidad(self):
        self.datos_carga_cese_investigador.set_documento_identidad(self.data.get("NIF"))

    def set_tipo(self):
        self.datos_carga_cese_investigador.set_tipo(self.data.get("ID_CESE"))

    def set_valor(self):
        self.datos_carga_cese_investigador.set_valor(self.data.get("DES_CESE"))

    def set_fecha(self):
        self.datos_carga_cese_investigador.set_fecha(
            self.data.get("F_CESE").strftime("%d/%m/%Y")
        )

    def carga(self):
        self.set_fuente_datos()
        self.set_documento_identidad()
        self.set_tipo()
        self.set_valor()
        self.set_fecha()
        self.datos_carga_cese_investigador.close()
