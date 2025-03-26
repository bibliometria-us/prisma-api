from abc import ABC, abstractmethod
import json
from db.conexion import BaseDatos

from utils.format import enumerated_dict
from json import JSONEncoder
from typing import Any, Type


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
        self.categoria: DatosCargaCategoriaInvestigador = (
            DatosCargaCategoriaInvestigador()
        )
        self.area: DatosCargaAreaInvestigador = DatosCargaAreaInvestigador()
        self.departamento: DatosCargaDepartamentoInvestigador = (
            DatosCargaDepartamentoInvestigador()
        )
        self.centro: DatosCargaCentroInvestigador = DatosCargaCentroInvestigador()
        self.centro_censo: DatosCargaCentroCensoInvestigador = (
            DatosCargaCentroCensoInvestigador()
        )
        self.ceses: list[DatosCargaCeseInvestigador] = []
        self.nacionalidad = ""
        self.sexo = ""
        # TODO: fechas como listas
        self.fechaNacimiento = ""
        self.fechaContratacion = ""
        self.fechaNombramiento = ""
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

    def add_cese(self, cese: "DatosCargaCeseInvestigador"):
        self.ceses.append(cese)

    def set_nacionalidad(self, nacionalidad: str):
        self.nacionalidad = nacionalidad

    def set_sexo(self, sexo: str):
        self.sexo = sexo

    def set_fechaNacimiento(self, fechaNacimiento: str):
        self.fechaNacimiento = fechaNacimiento

    def set_fechaContratacion(self, fechaContratacion: str):
        self.fechaContratacion = fechaContratacion

    def set_fechaNombramiento(self, fechaNombramiento: str):
        self.fechaNombramiento = fechaNombramiento

    def to_dict(self):
        result = {
            "fuente_datos": self.fuente_datos,
            "id": self.id,
            "nombre": self.nombre,
            "apellidos": self.apellidos,
            "documento_identidad": self.documento_identidad,
            "email": self.email,
            "categoria": self.categoria.to_dict(),
            "area": self.area.to_dict(),
            "departamento": self.departamento.to_dict(),
            "centro": self.centro.to_dict(),
            "centro_censo": self.centro_censo.to_dict(),
            "ceses": self.ceses.to_dict(),
            "nacionalidad": self.nacionalidad,
            "sexo": self.sexo,
            "fechaNacimiento": self.fechaNacimiento,
            "fechaContratacion": self.fechaContratacion,
            "fechaNombramiento": self.fechaNombramiento,
        }

        return result

    def from_dict(self, source: dict):
        self.fuente_datos = source.get("fuente_datos")
        self.id = source.get("id")
        self.nombre = source.get("nombre")
        self.apellidos = source.get("apellidos")
        self.documento_identidad = source.get("documento_identidad")
        self.email = source.get("email")
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
        self.ceses = DatosCargaCeseInvestigador.merged_from_dict(
            source=source.get("ceses"), object_class=DatosCargaCeseInvestigador
        )
        self.nacionalidad = source.get("nacionalidad")
        self.sexo = source.get("sexo")
        self.fechaNacimiento = source.get("fechaNacimiento")
        self.fechaContratacion = source.get("fechaContratacion")
        self.fechaNombramiento = source.get("fechaNombramiento")

        return self

    def to_json(self):
        json_data = json.dumps(self.dict, indent=4, ensure_ascii=False)
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
            and self.categoria == value.categoria
            and self.area == value.area
            and self.departamento == value.departamento
            and self.centro == value.centro
            and self.centro_censo == value.centro_censo
            and self.ceses == value.ceses
            and self.nacionalidad == value.nacionalidad
            and self.sexo == value.sexo
            and self.fechaNacimiento == value.fechaNacimiento
            and self.fechaContratacion == value.fechaContratacion
            and self.fechaNombramiento == value.fechaNombramiento
        )


class DatosCargaCeseInvestigador(DatosCarga):
    def __init__(
        self,
        documento_identidad: str = "",
        tipo: str = "",
        valor: str = "",
        fecha: str = "",
    ) -> None:
        self.documento_identidad = documento_identidad
        self.tipo = tipo
        self.valor = valor
        self.fecha = fecha

    def to_dict(self):
        dict = {
            "documento_identidad": self.documento_identidad,
            "tipo": self.tipo,
            "valor": self.valor,
            "fecha": self.fecha,
        }

        return dict

    def from_dict(self, source: dict):
        self.documento_identidad = source.get("documento_identidad")
        self.tipo = source.get("tipo")
        self.valor = source.get("valor")
        self.fecha = source.get("fecha")

        return self

    def __eq__(self, value: "DatosCargaCeseInvestigador") -> bool:
        return (
            self.documento_identidad == value.documento_identidad
            and self.tipo == value.tipo
            and self.valor == value.valor
            and self.fecha == value.fecha
        )

    def __hash__(self) -> int:
        return hash((self.documento_identidad, self.tipo, self.valor, self.fecha))


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
