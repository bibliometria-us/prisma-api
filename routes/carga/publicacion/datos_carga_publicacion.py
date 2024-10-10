import json
from db.conexion import BaseDatos
from utils.format import enumerated_dict
from json import JSONEncoder


class Encoder(JSONEncoder):
    def default(self, o):
        return o.to_dict()


class DatosCargaPublicacion:
    def __init__(self) -> None:
        self.titulo = ""
        self.titulo_alternativo = ""
        self.tipo = ""
        self.autores: list[CargaAutor] = list()
        self.año_publicacion: int = None
        self.fecha_publicacion: str = ""
        self.identificadores: list[CargaIdentificadorPublicacion] = list()
        self.datos: list[CargaDato] = list()
        self.revista: CargaRevista = CargaRevista()
        self.dict: dict = {}

    def set_titulo(self, titulo: str):
        assert titulo
        self.titulo = titulo

    def set_titulo_alternativo(self, titulo_alternativo: str):
        assert titulo_alternativo
        self.titulo_alternativo = titulo_alternativo

    def set_tipo(self, tipo: str):
        assert tipo
        self.tipo = tipo

    def set_revista(self, revista: "CargaRevista"):
        if self.tipo == "Libro":
            revista.tipo = "Libro"
            revista.titulo = self.titulo
        self.revista = revista

    def es_tesis(self) -> bool:
        return self.tipo == "Tesis"

    def set_año_publicacion(self, año: int):
        año_str = str(año)
        assert len(año_str) == 4 and (
            año_str.startswith("19") or año_str.startswith("20")
        )

        self.año_publicacion = año

    def set_fecha_publicacion(self, fecha: str):
        self.fecha_publicacion = fecha

    def add_autor(self, autor: "CargaAutor"):

        self.autores.append(autor)

    def add_identificador(self, identificador: "CargaIdentificadorPublicacion"):
        self.identificadores.append(identificador)

    def add_dato(self, dato: "CargaDato"):
        self.datos.append(dato)

    def libro_como_fuente(self):
        if self.tipo == "Libro":
            self.revista.tipo = "Libro"
            self.revista.titulo = self.titulo

    def to_dict(self):
        result = {
            "titulo": self.titulo,
            "titulo_alternativo": self.titulo_alternativo,
            "tipo": self.tipo,
            "autores": enumerated_dict(self.autores),
            "año_publicacion": self.año_publicacion,
            "fecha_publicacion": self.fecha_publicacion,
            "identificadores": enumerated_dict(self.identificadores),
            "datos": enumerated_dict(self.datos),
            "fuente": self.revista,
        }

        return result

    def to_json(self):
        json_data = json.dumps(self.dict, indent=4, ensure_ascii=False, cls=Encoder)
        return json_data

    def close(self):
        self.libro_como_fuente()
        self.dict = self.to_dict()


class CargaAutor:
    def __init__(self, firma: str, tipo: str, orden: int) -> None:
        self.firma = firma
        self.tipo = tipo
        self.orden = orden
        self.ids: list[IdAutor] = list()

    def add_id(self, id: "IdAutor"):
        self.ids.append(id)

    def to_dict(self):
        dict = {
            "firma": self.firma,
            "tipo": self.tipo,
            "orden": self.orden,
            "ids": enumerated_dict(self.ids),
        }

        return dict

    def __eq__(self, value: "CargaAutor") -> bool:
        return self.tipo == value.tipo and self.orden == value.orden

    def __hash__(self) -> int:
        return hash((self.firma, self.tipo, self.orden))


class IdAutor:
    def __init__(self, tipo: str, valor: str) -> None:
        self.tipo = tipo
        self.valor = valor

    def to_dict(self):
        dict = {
            "tipo": self.tipo,
            "valor": self.valor,
        }

        return dict

    def __eq__(self, value: "IdAutor") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))


class CargaIdentificadorPublicacion:
    def __init__(self, tipo: str, valor: str) -> None:
        self.tipo = tipo
        self.valor = valor

    def to_dict(self):
        dict = {
            "tipo": self.tipo,
            "valor": self.valor,
        }

        return dict

    def __eq__(self, value: "CargaIdentificadorPublicacion") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))


class CargaDato:
    def __init__(self, tipo: str, valor: str) -> None:
        self.tipo = tipo
        self.valor = valor

    def to_dict(self):
        dict = {
            "tipo": self.tipo,
            "valor": self.valor,
        }

        return dict

    def __eq__(self, value: "CargaIdentificadorPublicacion") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))


class CargaRevista:
    def __init__(self) -> None:
        self.titulo = ""
        self.tipo = ""
        self.editorial = ""
        self.identificadores: list[CargaIdentificadorRevista] = list()

    def set_titulo(self, titulo: str):
        self.titulo = titulo

    def set_tipo(self, tipo: str):
        self.tipo = tipo

    def set_editorial(self, editorial: str):
        self.editorial = editorial

    def add_identificador(self, identificador: "CargaIdentificadorRevista"):
        self.identificadores.append(identificador)

    def to_dict(self):
        dict = {
            "titulo": self.titulo,
            "tipo": self.tipo,
            "editorial": self.editorial,
            "identificadores": self.identificadores,
        }

        return dict


class CargaIdentificadorRevista:
    def __init__(self, tipo: str, valor: str) -> None:
        self.tipo = tipo
        self.valor = valor

    def to_dict(self):
        dict = {
            "tipo": self.tipo,
            "valor": self.valor,
        }

        return dict

    def __eq__(self, value: "CargaIdentificadorRevista") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))
