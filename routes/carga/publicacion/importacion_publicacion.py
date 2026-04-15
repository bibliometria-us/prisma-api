from routes.carga.publicacion.carga_publicacion import CargaPublicacion
from routes.carga.publicacion.crossref.carga import CargaPublicacionCrossref
from routes.carga.publicacion.exception import (
    ErrorCargaPublicacion,
    ErrorImportacionPublicacion,
)
from routes.carga.publicacion.idus.carga import CargaPublicacionIdus
from routes.carga.publicacion.openalex.carga import CargaPublicacionOpenalex
from routes.carga.publicacion.scopus.carga import CargaPublicacionScopus
from routes.carga.publicacion.wos.carga import CargaPublicacionWos


class ImportacionPublicacion:
    def __init__(self, id: str, tipo_id: str, autor: str):
        self.id = id
        self.tipo_id = tipo_id
        self.autor = autor
        self.cargas: list[type[CargaPublicacion]] = []
        self.agregar_cargas()
        self.errores = []
        self.id_publicacion = 0

    mapa_fuentes = {
        "wos": [CargaPublicacionWos],
        "pubmed": [CargaPublicacionWos],
        "scopus": [CargaPublicacionScopus],
        "openalex": [CargaPublicacionOpenalex],
        "crossref": [CargaPublicacionCrossref],
        "doi": [
            CargaPublicacionWos,
            CargaPublicacionScopus,
            CargaPublicacionOpenalex,
            CargaPublicacionCrossref,
        ],
        "idus": [CargaPublicacionIdus],
    }

    def agregar_cargas(self):
        clases_carga = self.mapa_fuentes.get(self.tipo_id)
        if not clases_carga:
            raise ErrorImportacionPublicacion(f"Fuente {self.tipo_id} no soportada.")
        self.cargas = clases_carga

    def importar(self):
        for clase_carga in self.cargas:
            carga = clase_carga(autor=self.autor, tipo_carga="importacion")
            try:
                id_publicacion = carga.carga_publicacion(tipo=self.tipo_id, id=self.id)
                if id_publicacion:
                    self.id_publicacion = id_publicacion
            except ErrorCargaPublicacion as e:
                self.errores.append(f"Error al importar desde {carga.origen}. {str(e)}")
