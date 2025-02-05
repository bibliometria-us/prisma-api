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
        return {index: value.to_dict() for index, value in indexed.items()}

    @staticmethod
    def merged_from_dict(source: dict[int, dict], object_class: Type["DatosCarga"]):
        if len(source) == 0:
            return {}

        return [object_class().from_dict(valor) for valor in source.values()]

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
        assert titulo
        self.titulo = titulo

    def set_titulo_alternativo(self, titulo_alternativo: str):
        assert titulo_alternativo
        self.titulo_alternativo = titulo_alternativo

    # TODO: varios tit alternativos
    # def add_titulo_alternativo(self, titulo_alternativo: str):
    #     assert titulo_alternativo
    #     self.titulo_alternativo.append(titulo_alternativo)

    def set_tipo(self, tipo: str):
        assert tipo
        self.tipo = tipo

    def set_revista(self, revista: "DatosCargaFuente"):
        if self.tipo == "Libro":
            revista.tipo = "Libro"
            revista.titulo = self.titulo
        self.fuente = revista

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
            "fuente": self.fuente.to_dict(self.fuente),
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
        self.financiacion = DatosCargaFinanciacion().from_dict(
            source=source.get("financiacion")
        )
        self.fechas_publicacion = DatosCargaFechaPublicacion().from_dict(
            source=source.get("fechas_publicacion")
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

    def close(self):
        self.libro_como_fuente()
        self.dict = self.to_dict()

    def __eq__(self, value: "DatosCargaPublicacion") -> bool:
        return (
            self.titulo == value.titulo
            and self.titulo_alternativo == value.titulo_alternativo
            and self.tipo == value.tipo
            and self.autores == value.autores
            and self.año_publicacion == value.año_publicacion
            and self.identificadores == value.identificadores
            and self.datos == value.datos
            and self.fuente == value.fuente
            and self.financiacion == value.financiacion
            and self.fechas_publicacion == value.fechas_publicacion
        )


class DatosCargaAutor(DatosCarga):
    def __init__(self, firma: str = "", tipo: str = "", orden: int = "") -> None:
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
            for identificador in ids.values()
        ]

        afiliaciones: dict[int, DatosCargaAfiliacionesAutor] = source.get(
            "afiliaciones"
        )
        self.afiliaciones = [
            DatosCargaAfiliacionesAutor().from_dict(afiliacion)
            for afiliacion in afiliaciones.values()
        ]

        return self

    def __eq__(self, value: "DatosCargaAutor") -> bool:
        return (
            self.tipo == value.tipo
            and self.orden == value.orden
            and self.contacto == value.contacto
            and self.afiliaciones == value.afiliaciones
        )

    def __hash__(self) -> int:
        return hash((self.firma, self.tipo, self.orden))


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
        return hash((self.nombre, self.pais, self.ciudad, self.ror_id))


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
    def __init__(self) -> None:
        self.id_fuente = 0
        self.titulo = ""
        self.tipo = ""
        self.editoriales: list[DatosCargaEditorial] = list()
        self.identificadores: list[DatosCargaIdentificadorFuente] = list()
        self.datos: list[DatosCargaDatosFuente] = list()

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

        self.editoriales = self.merged_from_dict(
            source.get("editoriales"), DatosCargaEditorial
        )
        self.identificadores = self.merged_from_dict(
            source.get("identificadores"), DatosCargaIdentificadorFuente
        )

        return self

    def __eq__(self, value: "DatosCargaFuente") -> bool:
        return (
            self.titulo == value.titulo
            and self.tipo == value.tipo
            and self.editoriales == value.editoriales
            and self.identificadores == value.identificadores
        )


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

    def __eq__(self, value: "DatosCargaIdentificadorFuente") -> bool:
        return self.tipo == value.tipo and self.valor == value.valor

    def __hash__(self) -> int:
        return hash((self.tipo, self.valor))


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

    def __eq__(self, value: "DatosCargaEditorial") -> bool:
        return (
            self.nombre == value.nombre
            and self.tipo == value.tipo
            and self.pais == value.pais
            and self.url == value.url
        )

    def __hash__(self) -> int:
        return hash(
            self.nombre,
            self.tipo,
            self.pais,
            self.url,
        )


class DatosCargaFinanciacion(DatosCarga):
    def __init__(
        self,
        entidad_financiadora: str = "",
        proyecto: str = "",
        pais: str = "",
        ror: str = "",
    ) -> None:
        self.proyecto = proyecto
        self.entidad_financiadora = entidad_financiadora
        self.pais = pais
        self.ror = ror

    def to_dict(self):
        dict = {
            "proyecto": self.proyecto,
            "entidad_financiadora": self.entidad_financiadora,
            "pais": self.pais,
            "ror": self.ror,
        }

        return dict

    def from_dict(self, source: dict):
        self.proyecto = source.get("proyecto")
        self.entidad_financiadora = source.get("entidad_financiadora")
        self.pais = source.get("pais")
        self.ror = source.get("ror")

        return self

    def __eq__(self, value: "DatosCargaFinanciacion") -> bool:
        return (
            self.proyecto == value.proyecto
            and self.entidad_financiadora == value.entidad_financiadora
            and self.pais == value.pais
            and self.ror == value.ror
        )

    def __hash__(self) -> int:
        return hash((self.proyecto, self.entidad_financiadora, self.pais, self.ror))


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


class DatosCargaFechaPublicacion(DatosCarga):
    def __init__(
        self,
        mes: str = "",
        agno: str = "",
        tipo: str = "",
    ) -> None:
        self.mes = mes
        self.agno = agno
        self.tipo = tipo

    def to_dict(self):
        dict = {
            "mes": self.mes,
            "agno": self.agno,
            "tipo": self.tipo,
        }

        return dict

    def from_dict(self, source: dict):
        self.mes = source.get("mes")
        self.agno = source.get("agno")
        self.tipo = source.get("tipo")

        return self

    def __eq__(self, value: "DatosCargaFechaPublicacion") -> bool:
        return (
            self.mes == value.mes
            and self.agno == value.agno
            and self.tipo == value.tipo
        )

    def __hash__(self) -> int:
        return hash((self.mes, self.agno, self.tipo))


def datos_publicacion_por_id(id_publicacion, db: BaseDatos) -> DatosCargaPublicacion:
    # Buscar publicacion por id e introducir sus atributos
    datos_carga_publicacion = DatosCargaPublicacion()

    query_publicacion = (
        "SELECT * FROM prisma.i_publicacion WHERE idPublicacion = %(idPublicacion)s"
    )

    params_publicacion = {"idPublicacion": id_publicacion}

    db.ejecutarConsulta(query_publicacion, params_publicacion)
    publicacion = db.get_dataframe().iloc[0]

    datos_carga_publicacion.fuente_datos = publicacion["origen"]
    datos_carga_publicacion.titulo = publicacion["titulo"]
    datos_carga_publicacion.tipo = publicacion["tipo"]
    datos_carga_publicacion.año_publicacion = publicacion["agno"]

    # Buscar e insertar autores
    lista_datos_autor: list[DatosCargaAutor] = []

    query_autores = (
        "SELECT * FROM prisma.p_autor WHERE idPublicacion = %(idPublicacion)s"
    )
    params_autores = {"idPublicacion": id_publicacion}

    db.ejecutarConsulta(query_autores, params_autores)
    autores = db.get_dataframe()

    for index, autor in autores.iterrows():
        dato_autor = DatosCargaAutor()

        dato_autor.firma = autor["firma"]
        dato_autor.tipo = autor["rol"]
        dato_autor.orden = autor["orden"]
        dato_autor.contacto = autor["contacto"]

        lista_datos_autor.append(dato_autor)

    datos_carga_publicacion.autores = lista_datos_autor

    # Buscar e insertar identificadores

    lista_datos_identificador_publicacion: list[DatosCargaIdentificadorPublicacion] = []

    query_identificadores_publicacion = "SELECT * FROM prisma.p_identificador_publicacion WHERE idPublicacion = %(idPublicacion)s"
    params_identificadores_publicacion = {"idPublicacion": id_publicacion}

    db.ejecutarConsulta(
        query_identificadores_publicacion, params_identificadores_publicacion
    )
    identificadores_publicacion = db.get_dataframe()

    for index, identificador_publicacion in identificadores_publicacion.iterrows():
        dato_identificador_publicacion = DatosCargaIdentificadorPublicacion()

        dato_identificador_publicacion.tipo = identificador_publicacion["rol"]
        dato_identificador_publicacion.valor = identificador_publicacion["orden"]

        lista_datos_identificador_publicacion.append(dato_identificador_publicacion)

    datos_carga_publicacion.identificadores = lista_datos_identificador_publicacion

    # Buscar e insertar datos

    lista_datos_dato_publicacion: list[DatosCargaDatoPublicacion] = []

    query_datoes_publicacion = "SELECT * FROM prisma.p_dato_publicacion WHERE idPublicacion = %(idPublicacion)s"
    params_datoes_publicacion = {"idPublicacion": id_publicacion}

    db.ejecutarConsulta(query_datoes_publicacion, params_datoes_publicacion)
    datos_publicacion = db.get_dataframe()

    for index, dato_publicacion in datos_publicacion.iterrows():
        dato_dato_publicacion = DatosCargaDatoPublicacion()

        dato_dato_publicacion.tipo = dato_publicacion["rol"]
        dato_dato_publicacion.valor = dato_publicacion["orden"]

        lista_datos_dato_publicacion.append(dato_dato_publicacion)

    datos_carga_publicacion.datos = lista_datos_dato_publicacion

    # Buscar e insertar fuente

    datos_carga_fuente = DatosCargaFuente()
    datos_carga_fuente.id_fuente = publicacion["idFuente"]

    query_fuente = "SELECT * FROM prisma.p_fuente WHERE idFuente = %(idFuente)s"
    params_query_fuente = {"idFuente": datos_carga_fuente.id_fuente}

    db.ejecutarConsulta(query_fuente, params_query_fuente)
    datos_fuente = db.get_dataframe().iloc[0]

    datos_carga_fuente.titulo = datos_fuente["titulo"]
    datos_carga_fuente.tipo = datos_fuente["tipo"]

    # Buscar e insertar editoriales de fuente

    lista_datos_carga_editorial: list[DatosCargaEditorial] = []

    query_editoriales = """SELECT * FROM prisma.p_editor e
                            INNER JOIN (
                                SELECT valor FROM prisma.p_dato_fuente WHERE tipo = 'editorial' 
                                                                    AND idFuente = %(idFuente)s
                                        ) df ON df.valor = e.idFuente
                        """
    params_query_editoriales = {"idFuente": datos_carga_fuente.id_fuente}

    db.ejecutarConsulta(query_editoriales, params_query_editoriales)
    editoriales = db.get_dataframe()

    for index, datos_editorial in editoriales.iterrows():
        datos_carga_editorial = DatosCargaEditorial()

        datos_carga_editorial.nombre = datos_editorial["nombre"]
        datos_carga_editorial.tipo = datos_editorial["tipo"]
        datos_carga_editorial.pais = datos_editorial["pais"]
        datos_carga_editorial.url = datos_editorial["url"]

        lista_datos_carga_editorial.append(datos_carga_editorial)

    datos_carga_publicacion.fuente.editoriales = lista_datos_carga_editorial

    # Buscar e insertar identificadores de fuente

    lista_datos_identificador_fuente: list[DatosCargaIdentificadorFuente] = []

    query_identificadores_fuente = (
        "SELECT * FROM prisma.p_identificador_fuente WHERE idFuente = %(idFuente)s"
    )
    params_query_identificadores_fuente = {"idFuente": datos_carga_fuente.id_fuente}

    db.ejecutarConsulta(
        query_identificadores_fuente, params_query_identificadores_fuente
    )
    identificadores_fuente = db.get_dataframe()

    for index, datos_identificador_fuente in identificadores_fuente.iterrows():
        datos_carga_identificador_fuente = DatosCargaIdentificadorFuente()

        datos_carga_identificador_fuente.tipo = datos_identificador_fuente["tipo"]
        datos_carga_identificador_fuente.valor = datos_identificador_fuente["valor"]

        lista_datos_identificador_fuente.append(datos_carga_identificador_fuente)

    datos_carga_publicacion.fuente.identificadores = lista_datos_identificador_fuente
