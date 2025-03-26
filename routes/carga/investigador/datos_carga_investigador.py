from abc import ABC, abstractmethod
import datetime
import json
from db.conexion import BaseDatos

from routes.carga.investigador.utils import validar_dni_nie, validar_email
from utils.format import enumerated_dict
from json import JSONEncoder
from typing import Any, Type
from datetime import datetime


class Encoder(JSONEncoder):
    def default(self, o: "DatosCarga"):
        return o.to_dict()


class DatosCarga(ABC):
    @abstractmethod
    def to_dict(self):
        pass

    @abstractmethod
    def from_dict(self, source: dict):
        pass

    @staticmethod
    def merge_dict(source: list["DatosCarga"] = []):
        indexed: dict[int, DatosCarga] = enumerated_dict(source)
        return [value.to_dict() for index, value in indexed.items()]

    @staticmethod
    def merged_from_dict(source: dict[int, dict], object_class: Type["DatosCarga"]):
        if len(source) == 0:
            return {}

        return [object_class().from_dict(valor) for valor in source]

    def __str__(self):
        return str(self.to_dict())


# *****************************
# ***** CLASE INVESTIGADOR ****
# *****************************
class DatosCargaInvestigador(DatosCarga):
    """
    Clase que representa los datos necesarios para un investigador en Prisma
    """

    def __init__(self) -> None:
        self.fuente_datos = ""
        self.id = ""
        self.nombre = ""
        self.apellidos = ""
        self.documento_identidad = ""
        self.email = ""
        self.nacionalidad = ""
        self.sexo = ""
        self.fecha_nacimiento = ""
        self.contratos: list[DatosCargaContratoInvestigador] = []
        self.dict: dict = {}

    def set_fuente_datos(self, fuente_datos: str):
        self.fuente_datos = fuente_datos

    def set_nombre(self, nombre: str):
        self.nombre = nombre

    def set_apellidos(self, apellidos: str):
        self.apellidos = apellidos

    def set_documento_identidad(self, documento_identidad: str):
        self.documento_identidad = documento_identidad

    def set_email(self, email: str):
        self.email = email

    def set_nacionalidad(self, nacionalidad: str):
        self.nacionalidad = nacionalidad

    def set_sexo(self, sexo: str):
        self.sexo = sexo

    def set_fecha_nacimiento(self, fecha_nacimiento: str):
        self.fecha_nacimiento = fecha_nacimiento

    def add_contrato(self, contrato: "DatosCargaContratoInvestigador"):
        self.contratos.append(contrato)

    def add_contrato_virtual(self):
        self.contratos.append(DatosCargaContratoInvestigador())

    def add_contrato_virtual_con_cese(self, cese: "DatosCargaCeseInvestigador"):
        contrato = DatosCargaContratoInvestigador()
        contrato.set_cese(cese)
        self.contratos.append(contrato)

    def get_last_contrato(self) -> "DatosCargaContratoInvestigador":
        # Filtrar contratos que tengan una fecha válida
        contratos_validos = [
            c for c in self.contratos if c.fecha_contratacion not in (None, "")
        ]

        # Si no hay contratos válidos, retornar None
        if not contratos_validos:
            return None

        # Obtener el contrato con la fecha_contratacion más reciente
        return max(contratos_validos, key=lambda c: c.fecha_contratacion)

    def get_nearest_contrato(self, fecha_cese: str) -> "DatosCargaContratoInvestigador":
        # Convertir fecha_cese de str a datetime
        fecha_cese_dt = datetime.strptime(fecha_cese, "%d/%m/%Y")

        # Filtrar contratos con fecha_contratacion válida y que sean menores o iguales a fecha_cese
        contratos_validos = [
            c
            for c in self.contratos
            if c.fecha_contratacion not in (None, "")  # Ignorar fechas vacías
            and datetime.strptime(c.fecha_contratacion, "%d/%m/%Y") <= fecha_cese_dt
        ]

        # Si no hay contratos válidos, retornar None
        if not contratos_validos:
            return None

        # Buscar el contrato más cercano a fecha_cese
        return min(
            contratos_validos,
            key=lambda c: (
                fecha_cese_dt - datetime.strptime(c.fecha_contratacion, "%d/%m/%Y")
            ).days,
        )

    def to_dict(self):
        result = {
            "fuente_datos": self.fuente_datos,
            "id": self.id,
            "nombre": self.nombre,
            "apellidos": self.apellidos,
            "documento_identidad": self.documento_identidad,
            "email": self.email,
            "nacionalidad": self.nacionalidad,
            "sexo": self.sexo,
            "fecha_nacimiento": self.fecha_nacimiento,
            "contratos": DatosCargaContratoInvestigador.merge_dict(self.contratos),
        }

        return result

    def from_dict(self, source: dict):
        self.fuente_datos = source.get("fuente_datos")
        self.id = source.get("id")
        self.nombre = source.get("nombre")
        self.apellidos = source.get("apellidos")
        self.documento_identidad = source.get("documento_identidad")
        self.email = source.get("email")
        self.nacionalidad = source.get("nacionalidad")
        self.sexo = source.get("sexo")
        self.fecha_nacimiento = source.get("fecha_nacimiento")
        self.contratos = DatosCargaContratoInvestigador.merged_from_dict(
            source=source.get("contratos"), object_class=DatosCargaContratoInvestigador
        )

        return self

    def to_json(self):
        json_data = json.dumps(self.to_dict(), indent=4, ensure_ascii=False)
        return json_data

    def from_json(self, json_data: str) -> "DatosCargaInvestigador":
        self.dict = json.loads(json_data)
        self.from_dict(self.dict)
        return self

    def close(self):
        self.dict = self.to_dict()

    def __eq__(self, value: "DatosCargaInvestigador") -> bool:
        return (
            self.fuente_datos == value.fuente_datos
            and self.id == value.id
            and self.nombre == value.nombre
            and self.apellidos == value.apellidos
            and self.documento_identidad == value.documento_identidad
            and self.email == value.email
            and self.nacionalidad == value.nacionalidad
            and self.sexo == value.sexo
            and self.fecha_nacimiento == value.fecha_nacimiento
            and self.contratos == value.contratos
        )

    def sanitize(self):
        pass

    def validation(self):
        # TODO: Cuidado con los investigadores virtuales.
        is_valid = True

        # Validar DNI/NIE
        is_valid = (
            is_valid
            and self.documento_identidad is not None
            and self.documento_identidad != ""
        )
        is_valid = is_valid and validar_dni_nie(self.documento_identidad)

        # Validar email (dominio por defecto: us.es)
        is_valid = is_valid and self.email is not None and self.email != ""
        is_valid = is_valid and validar_email(self.email)

        return is_valid


class DatosCargaContratoInvestigador(DatosCarga):
    """
    Clase que representa los datos de un contrato de un investigador en Prisma
    """

    def __init__(self) -> None:
        self.categoria: DatosCargaCategoriaInvestigador = (
            DatosCargaCategoriaInvestigador()
        )
        self.area: DatosCargaAreaInvestigador = DatosCargaAreaInvestigador()
        self.departamento: DatosCargaDepartamentoInvestigador = (
            DatosCargaDepartamentoInvestigador()
        )
        # TODO: Los centros son parte de la estructura de un contrato?
        self.centro: DatosCargaCentroInvestigador = DatosCargaCentroInvestigador()
        self.centro_censo: DatosCargaCentroCensoInvestigador = (
            DatosCargaCentroCensoInvestigador()
        )
        self.cese: DatosCargaCeseInvestigador = DatosCargaCeseInvestigador()
        self.fecha_contratacion = ""
        self.fecha_fin_contratacion = ""
        self.fecha_nombramiento = ""
        self.dict: dict = {}

    def set_categoria(self, categoria: "DatosCargaCategoriaInvestigador"):
        self.categoria = categoria

    def set_area(self, area: "DatosCargaAreaInvestigador"):
        self.area = area

    def set_departamento(self, departamento: "DatosCargaDepartamentoInvestigador"):
        self.departamento = departamento

    def set_centro(self, centro: "DatosCargaCentroInvestigador"):
        self.centro = centro

    def set_centro_censo(self, centro_censo: "DatosCargaCentroCensoInvestigador"):
        self.centro_censo = centro_censo

    def set_cese(self, cese: "DatosCargaCeseInvestigador"):
        self.cese = cese

    def set_fecha_contratacion(self, fecha_contratacion: str):
        self.fecha_contratacion = fecha_contratacion

    def set_fecha_fin_contratacion(self, fecha_fin_contratacion: str):
        self.fecha_fin_contratacion = fecha_fin_contratacion

    def set_fecha_nombramiento(self, fecha_nombramiento: str):
        self.fecha_nombramiento = fecha_nombramiento

    def to_dict(self):
        result = {
            "categoria": self.categoria.to_dict(),
            "area": self.area.to_dict(),
            "departamento": self.departamento.to_dict(),
            "centro": self.centro.to_dict(),
            "centro_censo": self.centro_censo.to_dict(),
            "cese": self.cese.to_dict(),
            "fecha_contratacion": self.fecha_contratacion,
            "fecha_fin_contratacion": self.fecha_fin_contratacion,
            "fecha_nombramiento": self.fecha_nombramiento,
        }

        return result

    def from_dict(self, source: dict):
        self.categoria = DatosCargaCategoriaInvestigador().from_dict(
            source=source.get("categoria")
        )
        self.area = DatosCargaAreaInvestigador().from_dict(source=source.get("area"))
        self.departamento = DatosCargaDepartamentoInvestigador().from_dict(
            source=source.get("departamento")
        )
        self.centro = DatosCargaCentroInvestigador().from_dict(
            source=source.get("centro")
        )
        self.centro_censo = DatosCargaCentroCensoInvestigador().from_dict(
            source=source.get("centro_censo")
        )
        self.cese = DatosCargaCeseInvestigador.from_dict(
            source=source.get("cese"), object_class=DatosCargaCeseInvestigador
        )
        self.fecha_contratacion = source.get("fecha_contratacion")
        self.fecha_fin_contratacion = source.get("fecha_fin_contratacion")
        self.fecha_nombramiento = source.get("fecha_nombramiento")

        return self

    def to_json(self):
        json_data = json.dumps(self.to_dict(), indent=4, ensure_ascii=False)
        return json_data

    def from_json(self, json_data: str) -> "DatosCargaContratoInvestigador":
        self.dict = json.loads(json_data)
        self.from_dict(self.dict)
        return self

    def close(self):
        self.dict = self.to_dict()

    def __eq__(self, value: "DatosCargaContratoInvestigador") -> bool:
        return (
            self.categoria == value.categoria
            and self.area == value.area
            and self.departamento == value.departamento
            and self.centro == value.centro
            and self.centro_censo == value.centro_censo
            and self.cese == value.cese
            and self.fecha_contratacion == value.fecha_contratacion
            and self.fecha_fin_contratacion == value.fecha_fin_contratacion
            and self.fecha_nombramiento == value.fecha_nombramiento
        )

    # funcion que devuelve si el contrato es virtual, es decir, cuando está
    # creado especificamente para un cese de un investigador que no esté en la lista de investigadores activos
    def es_virtual(self):
        return (
            self.fecha_contratacion == ""
            and self.fecha_fin_contratacion == ""
            and self.fecha_nombramiento == ""
        )

    def sanitize(self):
        pass

    def validation(self):
        is_valid = True

        # Validar Departamento
        is_valid = (
            is_valid
            and self.departamento.id is not None
            and self.departamento.id != ""
            and self.departamento.nombre is not None
            and self.departamento.nombre != ""
        )

        # Validar fechas
        is_valid = (
            is_valid
            and self.fecha_contratacion is not None
            and self.fecha_contratacion != ""
        )
        is_valid = (
            is_valid
            and self.fecha_nombramiento is not None
            and self.fecha_nombramiento != ""
        )

        return is_valid


class DatosCargaCategoriaInvestigador(DatosCarga):
    def __init__(
        self, id: str = "", nombre: str = "", femenino: str = "", tipo_pp: str = ""
    ) -> None:
        self.id = id
        self.nombre = nombre
        self.femenino = femenino
        self.tipo_pp = tipo_pp

    def to_dict(self):
        dict = {
            "id": self.id,
            "nombre": self.nombre,
            "femenino": self.femenino,
            "tipo_pp": self.tipo_pp,
        }

        return dict

    def from_dict(self, source: dict):
        self.id = source.get("id")
        self.nombre = source.get("nombre")
        self.femenino = source.get("femenino")
        self.tipo_pp = source.get("tipo_pp")

        return self

    def __eq__(self, value: "DatosCargaCategoriaInvestigador") -> bool:
        return (
            self.id == value.id
            and self.nombre == value.nombre
            and self.femenino == value.femenino
            and self.tipo_pp == value.tipo_pp
        )

    def __hash__(self) -> int:
        return hash((self.id, self.nombre, self.femenino, self.tipo_pp))


class DatosCargaAreaInvestigador(DatosCarga):
    def __init__(self, id: str = "", nombre: str = "") -> None:
        self.id = id
        self.nombre = nombre

    def to_dict(self):
        dict = {
            "id": self.id,
            "nombre": self.nombre,
        }

        return dict

    def from_dict(self, source: dict):
        self.id = source.get("id")
        self.nombre = source.get("nombre")

        return self

    def __eq__(self, value: "DatosCargaAreaInvestigador") -> bool:
        return self.id == value.id and self.nombre == value.nombre

    def __hash__(self) -> int:
        return hash((self.id, self.nombre))


class DatosCargaDepartamentoInvestigador(DatosCarga):
    def __init__(self, id: str = "", nombre: str = "") -> None:
        self.id = id
        self.nombre = nombre

    def to_dict(self):
        dict = {
            "id": self.id,
            "nombre": self.nombre,
        }

        return dict

    def from_dict(self, source: dict):
        self.id = source.get("id")
        self.nombre = source.get("nombre")

        return self

    def __eq__(self, value: "DatosCargaDepartamentoInvestigador") -> bool:
        return self.id == value.id and self.nombre == value.nombre

    def __hash__(self) -> int:
        return hash((self.id, self.nombre))


class DatosCargaCentroInvestigador(DatosCarga):
    def __init__(self, id: str = "", nombre: str = "") -> None:
        self.id = id
        self.nombre = nombre

    def to_dict(self):
        dict = {
            "id": self.id,
            "nombre": self.nombre,
        }

        return dict

    def from_dict(self, source: dict):
        self.id = source.get("id")
        self.nombre = source.get("nombre")

        return self

    def __eq__(self, value: "DatosCargaCentroInvestigador") -> bool:
        return self.id == value.id and self.nombre == value.nombre

    def __hash__(self) -> int:
        return hash((self.id, self.nombre))


class DatosCargaCentroCensoInvestigador(DatosCarga):
    def __init__(self, id: str = "", nombre: str = "") -> None:
        self.id = id
        self.nombre = nombre

    def to_dict(self):
        dict = {
            "id": self.id,
            "nombre": self.nombre,
        }

        return dict

    def from_dict(self, source: dict):
        self.id = source.get("id")
        self.nombre = source.get("nombre")

        return self

    def __eq__(self, value: "DatosCargaCentroCensoInvestigador") -> bool:
        return self.id == value.id and self.nombre == value.nombre

    def __hash__(self) -> int:
        return hash((self.id, self.nombre))


# ***********************
# ****  CLASE CESE   ****
# ***********************
class DatosCargaCeseInvestigador(DatosCarga):
    def __init__(self) -> None:
        self.fuente_datos = ""
        self.documento_identidad = ""
        self.tipo = ""
        self.valor = ""
        self.fecha = ""

    def set_fuente_datos(self, fuente_datos: str):
        self.fuente_datos = fuente_datos

    def set_documento_identidad(self, documento_identidad: str):
        self.documento_identidad = documento_identidad

    def set_tipo(self, tipo: str):
        self.tipo = tipo

    def set_valor(self, valor: str):
        self.valor = valor

    def set_fecha(self, fecha: str):
        self.fecha = fecha

    def to_dict(self):
        dict = {
            "fuente_datos": self.fuente_datos,
            "documento_identidad": self.documento_identidad,
            "tipo": self.tipo,
            "valor": self.valor,
            "fecha": self.fecha,
        }

        return dict

    def from_dict(self, source: dict):
        self.fuente_datos = source.get("fuente_datos")
        self.documento_identidad = source.get("documento_identidad")
        self.tipo = source.get("tipo")
        self.valor = source.get("valor")
        self.fecha = source.get("fecha")

        return self

    def to_json(self):
        json_data = json.dumps(self.to_dict(), indent=4, ensure_ascii=False)
        return json_data

    def from_json(self, json_data: str) -> "DatosCargaInvestigador":
        self.dict = json.loads(json_data)
        self.from_dict(self.dict)
        return self

    def __eq__(self, value: "DatosCargaCeseInvestigador") -> bool:
        return (
            self.fuente_datos == value.fuente_datos
            and self.documento_identidad == value.documento_identidad
            and self.tipo == value.tipo
            and self.valor == value.valor
            and self.fecha == value.fecha
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.fuente_datos,
                self.documento_identidad,
                self.tipo,
                self.valor,
                self.fecha,
            )
        )

    def close(self):
        self.dict = self.to_dict()
