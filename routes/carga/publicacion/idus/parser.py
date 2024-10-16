from integration.apis.idus.idus import IdusAPIItems
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaAutor,
    DatosCargaDatoPublicacion,
    DatosCargaEditorial,
    DatosCargaIdentificadorPublicacion,
    DatosCargaIdentificadorFuente,
    DatosCargaIdentificadorAutor,
)
from routes.carga.publicacion.parser import Parser


class IdusParser(Parser):
    def __init__(self, handle: str) -> None:
        super().__init__()
        self.handle = handle
        self.data: dict = None
        self.api_request()
        self.metadata: dict = self.data.get("metadata")
        self.carga()

    def set_fuente_datos(self):
        self.datos_carga_publicacion.set_fuente_datos("IDUS")

    def api_request(self):
        api = IdusAPIItems()
        response = api.get_from_handle(self.handle)

        self.data = response

    def cargar_titulo(self):
        titulo = self.metadata["dc.title"][0]["value"]
        self.datos_carga_publicacion.set_titulo(titulo)

    def cargar_titulo_alternativo(self):
        titulo = self.metadata.get("dc.title.alternative")
        if not titulo:
            return None

        valor = titulo[0]["value"]
        self.datos_carga_publicacion.set_titulo_alternativo(valor)

    def cargar_tipo(self):
        tipo = self.metadata["dc.type"][0]["value"]

        tipos = {
            "info:eu-repo/semantics/article": "Artículo",
            "info:eu-repo/semantics/conferenceObject": "Ponencia",
            "info:eu-repo/semantics/bookPart": "Capítulo",
            "info:eu-repo/semantics/book": "Libro",
            "info:eu-repo/semantics/doctoralThesis": "Tesis",
            "info:eu-repo/semantics/dataset": "Dataset",
        }

        valor = tipos.get(tipo) or "Otros"
        self.datos_carga_publicacion.set_tipo(valor)

    def _cargar_autores(
        self,
        tipo: str,
        attr_name: str,
    ):
        if not self.metadata.get(attr_name):
            return None

        for autor in self.metadata.get(attr_name):

            firma = autor["value"]
            orden = autor["place"] + 1

            carga_autor = DatosCargaAutor(orden=orden, firma=firma, tipo=tipo)

            tipo_id = "idus"
            valor_id = autor["value"]

            id_autor = DatosCargaIdentificadorAutor(tipo=tipo_id, valor=valor_id)

            carga_autor.add_id(id_autor)

            self.datos_carga_publicacion.add_autor(carga_autor)

    def cargar_autores(self):
        self._cargar_autores(
            tipo="Autor/a",
            attr_name="dc.creator",
        )

    def cargar_editores(self):
        self._cargar_autores(
            tipo="Editor/a",
            attr_name="dc.contributor.editor",
        )

    def cargar_directores(self):
        self._cargar_autores(
            tipo="Director/a",
            attr_name="dc.contributor.advisor",
        )

    def cargar_año_publicacion(self):
        año = self.metadata["dc.date.issued"][0]["value"][0:4]
        assert len(año) == 4

        self.datos_carga_publicacion.set_año_publicacion(año)

    def cargar_fecha_publicacion(self):
        fecha = self.metadata["dc.date.available"][0]["value"]
        self.datos_carga_publicacion.set_fecha_publicacion(fecha)

    def cargar_doi(self):
        doi: dict = self.metadata.get("dc.identifier.doi")
        if not doi:
            return None

        valor = doi[0]["value"]
        identificador = DatosCargaIdentificadorPublicacion(valor=valor, tipo="doi")
        self.datos_carga_publicacion.add_identificador(identificador)

    def cargar_idus(self):
        idus: str = self.handle
        assert idus.startswith("11441/")

        identificador = DatosCargaIdentificadorPublicacion(valor=idus, tipo="idus")
        self.datos_carga_publicacion.add_identificador(identificador)

    def cargar_identificadores(self):
        self.cargar_doi()
        self.cargar_idus()

    def _cargar_dato(self, tipo: str, attr_name: str):
        valor = self.metadata.get(attr_name)
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo=tipo, valor=valor[0]["value"])

        self.datos_carga_publicacion.add_dato(dato)

    def cargar_volumen(self):
        self._cargar_dato(tipo="volumen", attr_name="dc.publication.volumen")

    def cargar_numero(self):
        self._cargar_dato(tipo="numero", attr_name="dc.publication.issue")

    def cargar_pag_inicio(self):
        self._cargar_dato(tipo="pag_inicio", attr_name="dc.publication.initialPage")

    def cargar_pag_fin(self):
        self._cargar_dato(tipo="pag_fin", attr_name="dc.publication.endPage")

    def cargar_datos(self):
        if self.datos_carga_publicacion.es_tesis():
            return None
        self.cargar_volumen()
        self.cargar_numero()
        self.cargar_pag_inicio()
        self.cargar_pag_fin()

    def cargar_issn(self):
        issn: dict = self.metadata.get("dc.identifier.issn")
        if not issn:
            return None

        valor = issn[0]["value"]
        identificador = DatosCargaIdentificadorFuente(valor=valor, tipo="issn")
        self.datos_carga_publicacion.fuente.add_identificador(identificador)

    def cargar_isbn(self):
        isbn: dict = self.metadata.get("dc.identifier.isbn")
        if not isbn:
            return None

        valor = isbn[0]["value"]
        identificador = DatosCargaIdentificadorFuente(valor=valor, tipo="isbn")
        self.datos_carga_publicacion.fuente.add_identificador(identificador)

    def cargar_titulo_y_tipo(self):
        titulo_revista = self.metadata.get("dc.journaltitle")
        titulo_libro = self.metadata.get("dc.relation.ispartof")
        titulo_congreso = self.metadata.get("dc.eventtitle")

        if titulo_revista:
            self.datos_carga_publicacion.fuente.set_titulo(titulo_revista[0]["value"])
            self.datos_carga_publicacion.fuente.set_tipo("Revista")
        if titulo_libro:
            self.datos_carga_publicacion.fuente.set_titulo(titulo_libro[0]["value"])
            self.datos_carga_publicacion.fuente.set_tipo("Libro")
        if titulo_congreso:
            self.datos_carga_publicacion.fuente.set_titulo(titulo_congreso[0]["value"])
            self.datos_carga_publicacion.fuente.set_tipo("Congreso")

    def carga_editorial(self):
        valor = self.metadata.get("dc.publisher")
        if not valor:
            return None
        for i in range(0, len(valor)):
            dato_editorial = DatosCargaEditorial(nombre=valor[i]["value"])
            self.datos_carga_publicacion.fuente.add_editorial(dato_editorial)

    def cargar_fuente(self):
        if self.datos_carga_publicacion.es_tesis():
            return None
        self.cargar_issn()
        self.cargar_isbn()
        self.cargar_titulo_y_tipo()
        self.carga_editorial()
