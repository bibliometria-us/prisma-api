from routes.carga.investigador.datos_carga_investigador import (
    DatosCargaInvestigador,
    DatosCargaCategoriaInvestigador,
    DatosCargaAreaInvestigador,
    DatosCargaDepartamentoInvestigador,
    DatosCargaCentroInvestigador,
    DatosCargaCeseInvestigador,
    DatosCargaContratoInvestigador,
)
from routes.carga.investigador.parser import ParserCese, ParserInvestigador
from datetime import datetime
import pandas as pd


# *****************************
# **** PARSER INVESTIGADOR ****
# *****************************
class ParserInvestigadorRRHH(ParserInvestigador):
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
        email = self.data.get("CORREO_ELECTRÓNICO")
        if pd.isna(email):
            email = None
        self.datos_carga_investigador.set_email(email)

    def cargar_nacionalidad(self):
        nacionalidad = self.data.get("NACIONALIDAD")
        if nacionalidad and not pd.isna(nacionalidad):
            self.datos_carga_investigador.set_nacionalidad(nacionalidad.title())
        else:
            self.datos_carga_investigador.set_nacionalidad("")

    def cargar_sexo(self):
        sexo = self.data.get("SEXO")
        map_sexo = {"V": 1, "M": 0}
        sexo_bd = map_sexo.get(sexo, 3)
        self.datos_carga_investigador.set_sexo(sexo_bd)

    def cargar_fecha_nacimiento(self):
        self.datos_carga_investigador.set_fecha_nacimiento(
            self.data.get("F_NACIMIENTO")
        )

    def cargar_contrato(self):
        contrato = DatosCargaContratoInvestigador()
        contrato.set_fecha_contratacion(self.data.get("F_INICIO").date())
        contrato.set_fecha_nombramiento(self.data.get("F_NOMBRAMIENTO").date())
        fecha_fin = self.data.get("F_FIN").date()
        if not pd.isna(fecha_fin):
            contrato.set_fecha_fin_contratacion(fecha_fin)
        else:
            contrato.set_fecha_fin_contratacion(None)
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

        map_id_area = {"AREA00": 0}
        id_area = self.data.get("AREA")
        nombre_area = self.data.get("DES_AREA")
        if pd.isna(id_area):
            id_area = 0
            nombre_area = "Sin área de conocimiento"
        id_area = map_id_area.get(id_area, id_area)
        id_area = int(id_area)

        area = DatosCargaAreaInvestigador(id="", nombre="")
        if not pd.isna(id_area) and not pd.isna(nombre_area):
            area = DatosCargaAreaInvestigador(id=id_area, nombre=nombre_area)
        contrato.set_area(area)

        # CENTRO CENSO --> pass
        self.datos_carga_investigador.add_contrato(contrato)


# *****************************
# ******** PARSER CESE ********
# *****************************
class ParserCeseRRHH(ParserCese):
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
        self.datos_carga_cese_investigador.set_fecha(self.data.get("F_CESE").date())
