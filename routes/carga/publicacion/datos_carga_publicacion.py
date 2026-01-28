from abc import ABC, abstractmethod
import copy
import json
from db.conexion import BaseDatos

from utils.format import enumerated_dict, truncate_string
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


class DatosCargaPublicacion(DatosCarga):
    """
    Clase que representa los datos necesarios para una publicacion en Prisma
    """

    def __init__(self) -> None:
        self.fuente_datos = ""
        self.titulo = ""
        self.titulo_alternativo = ""
        # self.titulo_alternativo = list[str] = list()
        # TODO: Puede haber mas de un titulo alternativo
        self.tipo = ""
        self.autores: list[DatosCargaAutor] = list()
        self.año_publicacion: str = ""
        self.identificadores: list[DatosCargaIdentificadorPublicacion] = list()
        self.datos: list[DatosCargaDatoPublicacion] = list()
        self.financiacion: list[DatosCargaFinanciacion] = list()
        self.fechas_publicacion: list[DatosCargaFechaPublicacion] = list()
        self.acceso_abierto: list[DatosCargaAccesoAbierto] = list()
        self.fuente: DatosCargaFuente = DatosCargaFuente()
        self.dict: dict = {}
        # TODO: APCs: prioridad alta
        # TODO: keywords

    def set_fuente_datos(self, fuente_datos: str):
        self.fuente_datos = fuente_datos

    def set_titulo(self, titulo: str):
        self.titulo = titulo

    def set_titulo_alternativo(self, titulo_alternativo: str):
        self.titulo_alternativo = titulo_alternativo

    # TODO: varios tit alternativos
    # def add_titulo_alternativo(self, titulo_alternativo: str):
    #     assert titulo_alternativo
    #     self.titulo_alternativo.append(titulo_alternativo)

    def set_tipo(self, tipo: str):
        assert tipo
        self.tipo = tipo

    def es_tesis(self) -> bool:
        return self.tipo == "Tesis"

    def set_agno_publicacion(self, agno: str):
        self.año_publicacion = agno

    def add_fechas_publicacion(self, fecha: "DatosCargaFechaPublicacion"):
        self.fechas_publicacion.append(fecha)

    def add_autor(self, autor: "DatosCargaAutor"):
        self.autores.append(autor)

    def add_identificador(self, identificador: "DatosCargaIdentificadorPublicacion"):
        self.identificadores.append(identificador)

    def add_dato(self, dato: "DatosCargaDatoPublicacion"):
        self.datos.append(dato)

    def add_financiacion(self, financiacion: "DatosCargaFinanciacion"):
        self.financiacion.append(financiacion)

    def add_acceso_abierto(self, acceso_abierto: "DatosCargaAccesoAbierto"):
        self.acceso_abierto.append(acceso_abierto)

    def libro_como_fuente(self):
        if self.tipo == "Libro":
            self.fuente.tipo = "Libro"
            self.fuente.titulo = self.titulo

    def contar_autores_agrupados(self) -> dict[str, int]:
        autores_agrupados = {}
        for autor in self.autores:
            if autor.tipo not in autores_agrupados:
                autores_agrupados[autor.tipo] = 0
            autores_agrupados[autor.tipo] += 1

        return dict(sorted(autores_agrupados.items()))

    def to_dict(self):
        result = {
            "titulo": self.titulo,
            "titulo_alternativo": self.titulo_alternativo,
            "tipo": self.tipo,
            "autores": self.merge_dict(self.autores),
            "año_publicacion": self.año_publicacion,
            "identificadores": self.merge_dict(self.identificadores),
            "datos": self.merge_dict(self.datos),
            "financiacion": self.merge_dict(self.financiacion),
            "fechas_publicacion": self.merge_dict(self.fechas_publicacion),
            "fuente": self.fuente.to_dict(),
            "acceso_abierto": self.merge_dict(self.acceso_abierto),
        }

        return result

    def from_dict(self, source: dict):
        self.titulo = source.get("titulo")
        self.titulo_alternativo = source.get("titulo_alternativo")
        self.tipo = source.get("tipo")

        self.autores = self.merged_from_dict(source.get("autores"), DatosCargaAutor)
        self.identificadores = self.merged_from_dict(
            source.get("identificadores"), DatosCargaIdentificadorPublicacion
        )
        self.datos = self.merged_from_dict(
            source.get("datos"), DatosCargaDatoPublicacion
        )

        self.año_publicacion = source.get("año_publicacion")
        self.fuente = DatosCargaFuente().from_dict(source=source.get("fuente"))
        self.financiacion = self.merged_from_dict(
            source.get("financiacion"), DatosCargaFinanciacion
        )
        self.fechas_publicacion = self.merged_from_dict(
            source.get("fechas_publicacion"), DatosCargaFechaPublicacion
        )
        self.acceso_abierto = self.merged_from_dict(
            source.get("acceso_abierto"), DatosCargaAccesoAbierto
        )

        return self

    def from_id_publicacion(
        self, id_publicacion: int, db: BaseDatos
    ) -> "DatosCargaPublicacion":
        from routes.carga.publicacion.buscar_datos_carga_publicacion import (
            busqueda_publicacion_por_id,
        )

        return busqueda_publicacion_por_id(id_publicacion=id_publicacion, db=db)

    def to_json(self):
        json_data = json.dumps(self.dict, indent=4, ensure_ascii=False)
        return json_data

    def from_json(self, json_data: str) -> "DatosCargaPublicacion":
        self.dict = json.loads(json_data)
        self.from_dict(self.dict)
        return self

    def es_capitulo(self):
        tipos = ["Capítulo"]
        return self.tipo in tipos

    def es_libro(self):
        tipos = ["Libro"]
        return self.tipo in tipos

    def normalizar_fuente(self):
        if not self.fuente.tiene_issn_e_isbn():
            return None
        if self.es_libro():
            self.fuente_a_coleccion()
            self.libro_como_fuente()
        if self.es_capitulo():
            self.fuente.coleccion.identificadores = self.fuente.get_issns()
            self.fuente.identificadores = self.fuente.get_isbns()

    def fuente_a_coleccion(self):
        self.fuente.coleccion = copy.deepcopy(self.fuente)
        # Limpiar isbns
        self.fuente.coleccion.identificadores = self.fuente.get_issns()

    def libro_como_fuente(self):
        self.fuente.titulo = self.titulo
        self.fuente.tipo = self.tipo
        # Limpiar issns
        self.fuente.identificadores = self.fuente.get_isbns()

    def sanitize(self):
        for financiacion in self.financiacion:
            financiacion.sanitize()
        for autor in self.autores:
            autor.sanitize()
        self.fuente.sanitize()
        self.sanitize_autores()

    def sanitize_autores(self):
        # Group authors by their 'tipo'
        autores_agrupados = {}
        for autor in self.autores:
            if autor.tipo not in autores_agrupados:
                autores_agrupados[autor.tipo] = []
                autores_agrupados[autor.tipo].append(autor)

        # Sort each group by 'orden' and normalize the 'orden' values
        for tipo, autores in autores_agrupados.items():
            autores.sort(key=lambda x: x.orden)
            for index, autor in enumerate(autores, start=1):
                autor.orden = index

    def validate(self):
        """Valida que los datos mínimos para una publicación estén presentes.
        Retorna True si los datos son válidos, False en caso contrario.
        Datos mínimos:
        - Título
        - Fuente válida (título y ISSN o ISBN)
        - Tipo (no puede ser "Tesis")
        - Al menos un autor
        """
        if not self.titulo:
            return False
        # Validar fuente: título, tipo y ISSN o ISBN
        if not self.fuente.validate():
            return False
        if not self.tipo or self.tipo == "Tesis":
            return False
        if not self.autores:
            return False
        return True

    def close(self):
        # self.libro_como_fuente()
        self.dict = self.to_dict()

    def __eq__(self, value: "DatosCargaPublicacion") -> bool:
        result = (
            self.titulo == value.titulo
            and self.tipo == value.tipo
            and set(self.autores) == set(value.autores)
            and self.año_publicacion == value.año_publicacion
            and set(self.identificadores) == set(value.identificadores)
            and set(self.datos) == set(value.datos)
            and self.fuente == value.fuente
            and set(self.financiacion) == set(value.financiacion)
            and set(self.fechas_publicacion) == set(value.fechas_publicacion)
        )

        return result


class DatosCargaAutor(DatosCarga):
    def __init__(self, firma: str = "", tipo: str = "", orden: int = "") -> None:
        self.id_autor = 0
        self.firma = firma
        self.tipo = tipo  # Tipología de autor: mínimo tipo Autor/a
        self.orden = orden
        self.contacto = "N"
        self.ids: list[DatosCargaIdentificadorAutor] = list()
        self.afiliaciones: list[DatosCargaAfiliacionesAutor] = list()

    def add_id(self, id: "DatosCargaIdentificadorAutor"):
        self.ids.append(id)

    def add_afiliacion(self, afiliacion: "DatosCargaAfiliacionesAutor"):
        self.afiliaciones.append(afiliacion)

    def set_contacto(self, contacto: str):
        assert contacto in ("S", "N")
        self.contacto = contacto

    def sanitize(self):
        for afiliacion in self.afiliaciones:
            afiliacion.sanitize()

    def to_dict(self):
        dict = {
            "firma": self.firma,
            "tipo": self.tipo,
            "orden": self.orden,
            "contacto": self.contacto,
            "ids": self.merge_dict(self.ids),
            "afiliaciones": self.merge_dict(self.afiliaciones),
        }

        return dict

    def from_dict(self, source: dict):
        self.firma = source.get("firma")
        self.tipo = source.get("tipo")
        self.orden = source.get("orden")
        self.contacto = source.get("contacto")

        ids: dict[int, DatosCargaIdentificadorAutor] = source.get("ids")
        self.ids = [
            DatosCargaIdentificadorAutor().from_dict(identificador)
            for identificador in ids
        ]

        afiliaciones: dict[int, DatosCargaAfiliacionesAutor] = source.get(
            "afiliaciones"
        )
        self.afiliaciones = [
            DatosCargaAfiliacionesAutor().from_dict(afiliacion)
            for afiliacion in afiliaciones
        ]

        return self

    def __eq__(self, value: "DatosCargaAutor") -> bool:
        result = (
            self.tipo == value.tipo
            and self.orden == value.orden
            and self.contacto == value.contacto
            and set(self.afiliaciones) == set(value.afiliaciones)
        )

        return result

    def __hash__(self) -> int:
        return hash((self.tipo, self.orden, self.contacto))


class DatosCargaIdentificadorAutor(DatosCarga):
    def __init__(self, tipo: str = "", valor: str = "") -> None:
        self.tipo = tipo
        self.valor = valor

    def to_dict(self):
        dict = {
            "tipo": self.tipo,
            "valor": self.valor,
        }

        return dict

    def from_dict(self, source: dict):
        self.tipo = source.get("tipo")
        self.valor = source.get("valor")

        return self

    def __eq__(self, value: "DatosCargaIdentificadorAutor") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))


class DatosCargaAfiliacionesAutor(DatosCarga):
    def __init__(
        self, nombre: str = "", pais: str = "", ciudad: str = "", ror_id: str = ""
    ) -> None:
        self.nombre = nombre
        self.pais = pais
        self.ciudad = ciudad
        # TODO: ror_vacio: controlar que si el ror_id sea vacio (ideal tabla identificador Aff)
        self.ror_id = ror_id

    def to_dict(self):
        dict = {
            "nombre": self.nombre,
            "pais": self.pais,
            "ciudad": self.ciudad,
            "ror_id": self.ror_id,
        }

        return dict

    def sanitize(self):
        self.pais = self.pais or "Desconocido"

    def from_dict(self, source: dict):
        self.nombre = source.get("nombre")
        self.pais = source.get("pais")
        self.ciudad = source.get("ciudad")
        self.ror_id = source.get("ror_id")

        return self

    def __eq__(self, value: "DatosCargaAfiliacionesAutor") -> bool:
        return (
            self.nombre == value.nombre and self.pais == value.pais
        ) or self.ror_id == value.ror_id

    def __hash__(self) -> int:
        return hash((self.ror_id))


class DatosCargaIdentificadorPublicacion(DatosCarga):
    def __init__(self, tipo: str = "", valor: str = "") -> None:
        self.tipo = tipo
        self.valor = valor

    def to_dict(self):
        dict = {
            "tipo": self.tipo,
            "valor": self.valor,
        }

        return dict

    def from_dict(self, source: dict):
        self.tipo = source.get("tipo")
        self.valor = source.get("valor")

        return self

    def __eq__(self, value: "DatosCargaIdentificadorPublicacion") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))


class DatosCargaDatoPublicacion(DatosCarga):
    def __init__(self, tipo: str = "", valor: str = "") -> None:
        self.tipo = tipo
        self.valor = valor

    def to_dict(self):
        dict = {
            "tipo": self.tipo,
            "valor": self.valor,
        }

        return dict

    def from_dict(self, source: dict):
        self.tipo = source.get("tipo")
        self.valor = source.get("valor")

        return self

    def __eq__(self, value: "DatosCargaIdentificadorPublicacion") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))


class DatosCargaFuente(DatosCarga):
    def __init__(self, coleccion=True) -> None:
        self.id_fuente = 0
        self.titulo = ""
        self.tipo = ""
        self.editoriales: list[DatosCargaEditorial] = list()
        self.identificadores: list[DatosCargaIdentificadorFuente] = list()
        self.datos: list[DatosCargaDatosFuente] = list()
        if coleccion:
            self.coleccion: DatosCargaFuente = DatosCargaFuente(coleccion=False)
        else:
            self.coleccion = None

    def set_titulo(self, titulo: str):
        self.titulo = titulo

    def set_tipo(self, tipo: str):
        self.tipo = tipo

    def add_editorial(self, editorial: "DatosCargaEditorial"):
        self.editoriales.append(editorial)

    def add_identificador(self, identificador: "DatosCargaIdentificadorFuente"):
        self.identificadores.append(identificador)

    def add_dato(self, datos: "DatosCargaDatosFuente"):
        self.datos.append(datos)

    def sanitize(self):
        self.titulo = truncate_string(self.titulo, 800)
        self.editorial = truncate_string(self.titulo, 200)
        for editorial in self.editoriales:
            editorial.sanitize()
        for identificador in self.identificadores:
            identificador.sanitize()

    def validate(self):
        """Valida que los datos mínimos para una fuente estén presentes.
        Retorna True si los datos son válidos, False en caso contrario.
        Datos mínimos:
        - Título
        - Tipo
        - Al menos un identificador: ISSN o ISBN
        """
        return self.titulo and self.tipo and (self.tiene_issn() or self.tiene_isbn())

    def get_issns(self) -> list["DatosCargaIdentificadorFuente"]:
        return [
            identificador
            for identificador in self.identificadores
            if identificador.tipo in ("issn", "eissn")
        ]

    def get_isbns(self) -> list["DatosCargaIdentificadorFuente"]:
        return [
            identificador
            for identificador in self.identificadores
            if identificador.tipo in ("isbn", "eisbn")
        ]

    def tiene_issn(self) -> bool:
        return any(
            identificador.tipo in ("issn", "eissn")
            for identificador in self.identificadores
        )

    def tiene_isbn(self) -> bool:
        return any(
            identificador.tipo in ("isbn", "eisbn")
            for identificador in self.identificadores
        )

    def tiene_issn_e_isbn(self) -> bool:
        return self.tiene_issn() and self.tiene_isbn()

    def to_dict(self):
        dict = {
            "titulo": self.titulo,
            "tipo": self.tipo,
            "editoriales": self.merge_dict(self.editoriales),
            "identificadores": self.merge_dict(self.identificadores),
            "datos": self.merge_dict(self.datos),
        }
        return dict

    def from_dict(self, source: dict):
        self.titulo = source.get("titulo")
        self.tipo = source.get("tipo")
        self.datos = self.merged_from_dict(source.get("datos"), DatosCargaDatosFuente)
        self.editoriales = self.merged_from_dict(
            source.get("editoriales"), DatosCargaEditorial
        )
        self.identificadores = self.merged_from_dict(
            source.get("identificadores"), DatosCargaIdentificadorFuente
        )

        return self

    def __eq__(self, value: "DatosCargaFuente") -> bool:
        result = (
            any(
                identificador in value.identificadores
                for identificador in self.identificadores
            )
            or self.identificadores == value.identificadores
        ) and self.coleccion == value.coleccion

        return result

    def __hash__(self):
        return hash((self.titulo, self.tipo, self.editoriales, self.identificadores))


class DatosCargaIdentificadorFuente(DatosCarga):
    def __init__(self, tipo: str = "", valor: str = "") -> None:
        self.tipo = tipo
        self.valor = valor

    def to_dict(self):
        dict = {
            "tipo": self.tipo,
            "valor": self.valor,
        }

        return dict

    def from_dict(self, source: dict):
        self.tipo = source.get("tipo")
        self.valor = source.get("valor")

        return self

    def sanitize(self):
        self.valor = "".join(filter(str.isdigit, self.valor))
        if self.tipo in ("issn", "eissn"):
            if len(self.valor) == 8:
                self.valor = f"{self.valor[:4]}-{self.valor[4:]}"

    def __eq__(self, value: "DatosCargaIdentificadorFuente") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))

    def __str__(self):
        return f"{self.tipo}: {self.valor}"


class DatosCargaDatosFuente(DatosCarga):
    def __init__(self, tipo: str = "", valor: str = "") -> None:
        self.tipo = tipo
        self.valor = valor

    def to_dict(self):
        dict = {
            "tipo": self.tipo,
            "valor": self.valor,
        }

        return dict

    def from_dict(self, source: dict):
        self.tipo = source.get("tipo")
        self.valor = source.get("valor")

        return self

    def __eq__(self, value: "DatosCargaDatosFuente") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))


class DatosCargaEditorial(DatosCarga):
    def __init__(self, nombre: str = "") -> None:
        self.id_editor = 0
        self.nombre = nombre
        self.tipo = "Otros"
        self.pais = "Desconocido"
        self.url = None

    def to_dict(self):
        dict = {
            "nombre": self.nombre,
            "tipo": self.tipo,
            "pais": self.pais,
            "url": self.url,
        }

        return dict

    def from_dict(self, source: dict):
        self.nombre = source.get("nombre")
        self.tipo = source.get("tipo")
        self.pais = source.get("pais")
        self.url = source.get("url")

        return self

    def sanitize(self):
        self.nombre = truncate_string(self.nombre, 200)

    def __eq__(self, value: "DatosCargaEditorial") -> bool:
        return (
            self.nombre == value.nombre
            and self.tipo == value.tipo
            and self.pais == value.pais
            and self.url == value.url
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.nombre,
                self.tipo,
                self.pais,
                self.url,
            )
        )


class DatosCargaFinanciacion(DatosCarga):
    def __init__(
        self,
        entidad_financiadora: str = "",
        agencia: str = "",
        proyecto: str = "",
        pais: str = "",
        ror: str = "",
    ) -> None:
        self.proyecto = proyecto
        self.entidad_financiadora = entidad_financiadora
        self.agencia = agencia
        self.pais = pais
        self.ror = ror

    def to_dict(self):
        dict = {
            "proyecto": self.proyecto,
            "entidad_financiadora": self.entidad_financiadora,
            "agencia": self.agencia,
            "pais": self.pais,
            "ror": self.ror,
        }

        return dict

    def from_dict(self, source: dict):
        self.proyecto = source.get("proyecto", "")
        self.entidad_financiadora = source.get("entidad_financiadora", "")
        self.agencia = source.get("agencia", "")
        self.pais = source.get("pais", "")
        self.ror = source.get("ror", "")

        return self

    def sanitize(self):
        """
        Sanitiza el atributo 'proyecto' asegurando que su longitud no exceda los 50 caracteres.
        Si la longitud de 'proyecto' es mayor a 50 caracteres, divide la cadena en partes
        y selecciona la parte más larga que tenga 50 caracteres o menos. Si no existe tal parte, trunca
        'proyecto' a los primeros 50 caracteres.
        """
        if self.proyecto and len(self.proyecto) > 50:
            parts = self.proyecto.split()
            longest_part = max(parts, key=len) if parts else self.proyecto[:50]
            self.proyecto = (
                longest_part if len(longest_part) <= 50 else self.proyecto[:50]
            )

        if self.agencia:
            self.agencia = self.agencia[:300]

    def __eq__(self, value: "DatosCargaFinanciacion") -> bool:
        return (self.proyecto == value.proyecto and self.agencia == value.agencia) or (
            self.ror == value.ror
        )

    def __hash__(self) -> int:
        return hash((self.proyecto))


class DatosCargaAccesoAbierto(DatosCarga):
    def __init__(self, valor: str = "", origen: str = "") -> None:
        self.valor = valor
        self.origen = origen

    def to_dict(self):
        dict = {
            "valor": self.valor,
            "origen": self.origen,
        }

        return dict

    def from_dict(self, source: dict):
        self.valor = source.get("valor")
        self.origen = source.get("origen")

        return self

    def __eq__(self, value: "DatosCargaAccesoAbierto") -> bool:
        return self.valor == value.valor and self.origen == value.origen

    def __hash__(self) -> int:
        return hash((self.valor, self.origen))

    def __str__(self):
        return f"{self.valor} ({self.origen})"


class DatosCargaFechaPublicacion(DatosCarga):
    def __init__(
        self,
        dia: str = None,
        mes: str = None,
        agno: str = None,
        tipo: str = "",
    ) -> None:
        self.dia = dia
        self.mes = mes
        self.agno = agno
        self.tipo = tipo

    def to_dict(self):
        dict = {
            "dia": self.dia,
            "mes": self.mes,
            "agno": self.agno,
            "tipo": self.tipo,
        }

        return dict

    def from_dict(self, source: dict):
        self.dia = source.get("dia")
        self.mes = source.get("mes")
        self.agno = source.get("agno")
        self.tipo = source.get("tipo")

        return self

    def __eq__(self, value: "DatosCargaFechaPublicacion") -> bool:
        return (
            self.dia == value.dia
            and self.mes == value.mes
            and self.agno == value.agno
            and self.tipo == value.tipo
        )

    def __hash__(self) -> int:
        return hash((self.dia, self.mes, self.agno, self.tipo))

    def __str__(self):
        str_agno = str(self.agno)
        str_mes = str(self.mes).zfill(2) if str(self.mes) else ""
        return f"{str_agno}{'-' + str_mes}"
