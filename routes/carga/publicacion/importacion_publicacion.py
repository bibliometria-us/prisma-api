from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.crossref.carga import ExtraccionPublicacionCrossref
from routes.carga.publicacion.exception import (
    ErrorCargaPublicacion,
    ErrorImportacionPublicacion,
)
from routes.carga.publicacion.extraccion_publicacion import ExtraccionPublicacion
from routes.carga.publicacion.idus.carga import ExtraccionPublicacionIdus
from routes.carga.publicacion.openalex.carga import ExtraccionPublicacionOpenalex
from routes.carga.publicacion.scopus.carga import ExtraccionPublicacionScopus
from routes.carga.publicacion.wos.carga import ExtraccionPublicacionWos


class ImportacionPublicacion:
    def __init__(self, id: str, tipo_id: str, autor: str):
        self.id = id
        self.tipo_id = tipo_id
        self.autor = autor
        self.cargas: list[type[ExtraccionPublicacion]] = []
        self.agregar_cargas()
        self.errores = []
        self.id_publicacion = 0

    mapa_fuentes = {
        "wos": [ExtraccionPublicacionWos],
        "pubmed": [ExtraccionPublicacionWos],
        "scopus": [ExtraccionPublicacionScopus],
        "openalex": [ExtraccionPublicacionOpenalex],
        "crossref": [ExtraccionPublicacionCrossref],
        "doi": [
            ExtraccionPublicacionWos,
            ExtraccionPublicacionScopus,
            ExtraccionPublicacionOpenalex,
            ExtraccionPublicacionCrossref,
        ],
        "idus": [ExtraccionPublicacionIdus],
    }

    def agregar_cargas(self):
        clases_carga = self.mapa_fuentes.get(self.tipo_id)
        if not clases_carga:
            raise ErrorImportacionPublicacion(f"Fuente {self.tipo_id} no soportada.")
        self.cargas = clases_carga

    def importar(self):
        for clase_extraccion in self.cargas:
            extraccion = clase_extraccion(autor=self.autor, tipo_carga="importacion")
            try:
                id_publicacion = extraccion.carga_publicacion(
                    tipo=self.tipo_id, id=self.id
                )
                if id_publicacion:
                    self.id_publicacion = id_publicacion
            except ErrorCargaPublicacion as e:
                self.errores.append(
                    f"Error al importar desde {extraccion.carga.origen}. {str(e)}"
                )
