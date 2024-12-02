from integration.apis.clarivate.wos.wos_api import WosAPI
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaAutor,
    DatosCargaDatoPublicacion,
    DatosCargaEditorial,
    DatosCargaIdentificadorPublicacion,
    DatosCargaIdentificadorFuente,
    DatosCargaIdentificadorAutor,
)
from routes.carga.publicacion.parser import Parser
from datetime import datetime


class WosParser(Parser):
    def __init__(self, idWos: str) -> None:
        # Se inicializa la clase padre
        # La clase padre Parser tiene el atributo datos_carga_publicacion
        super().__init__()
        # Se definen los atributos de la clase
        self.idWos = idWos
        self.data: dict = None
        self.api_request()  # Se hace la petición de Wos
        self.carga()  # Con los datos recuperados, se rellena el objeto datos_carga_publicacion

    def set_fuente_datos(self):
        self.datos_carga_publicacion.set_fuente_datos("WOS")

    def api_request(self):
        api = WosAPI()
        response = api.get_from_id(self.idWos)

        self.data = response

    def cargar_titulo(self):
        titulo_dict = self.data["static_data"]["summary"]["titles"]
        titulo = next(
            (
                element.get("content")
                for element in titulo_dict["title"]
                if element.get("type") == "item"
            ),
            None,
        )
        self.datos_carga_publicacion.set_titulo(titulo)

    def cargar_titulo_alternativo(self):
        # Scopus no devuelve un título alternativo
        pass

    def cargar_tipo(self):
        tipo = self.data["static_data"]["summary"]["doctypes"]["doctype"]

        tipos = {
            "ar": "Artículo",
            "ip": "Artículo",
            "cp": "Ponencia",
            "re": "Revisión",
            "ed": "Editorial",
            "bk": "Libro",
            "no": "Nota",
            "ch": "Capítulo",
            "sh": "Short Survey",
            "er": "Corrección",
            "le": "Letter",
        }

        valor = tipos.get(tipo) or "Otros"
        self.datos_carga_publicacion.set_tipo(valor)

    def _cargar_autores(
        self,
        tipo: str,
        attr_name: str,
    ):
        # TODO: Controlar que no vengan los autores vacíos
        # Se extraen las afiliaciones de la publicacion
        afiliaciones_publicacion = dict()
        for aff_pub in self.data[0].get("affiliation"):
            afiliaciones_publicacion[aff_pub["afid"]] = {
                "nombre": aff_pub["affilname"],
                "pais": aff_pub["affiliation-country"],
            }

        for autor in self.data[0].get(attr_name):
            # Se completa el Objeto DatosCargaAutor(Autor)
            firma = autor["authname"]
            orden = autor["@seq"]
            carga_autor = DatosCargaAutor(orden=orden, firma=firma, tipo=tipo)

            # Se completa el Objeto DatosCargaIdentificadorAutor(Identificador del Autor)
            tipo_id = "scopus"
            valor_id = autor["authid"]
            id_autor = DatosCargaIdentificadorAutor(tipo=tipo_id, valor=valor_id)
            carga_autor.add_id(id_autor)  # Lo añadimos al objeto de Autor

            afiliaciones_autor = []
            # Se completa las el Objeto DatosCargaAfiliacion(Afiliaciones del Autor)
            for aff in autor.get("afid"):
                if_aff = aff.get("$")
                nombre_aff = afiliaciones_publicacion[if_aff]["nombre"]
                pais_aff = afiliaciones_publicacion[if_aff]["pais"]
                # afiliacion_autor = DatosCargaAfiliacion(id=id_aff, nombre= nombre_aff, pais_aff = pais_aff)
                # afiliaciones_autor.append(afiliacion_autor)
            # TODO: Implementar
            # carga_autor.add_afiliaciones(afiliaciones_autor)

            self.datos_carga_publicacion.add_autor(carga_autor)

    def cargar_autores(self):
        self._cargar_autores(
            tipo="Autor/a",
            attr_name="author",
        )

    def cargar_editores(self):
        pass

    def cargar_directores(self):
        pass

    def cargar_año_publicacion(self):
        año = datetime.strptime(self.data[0].get("prism:coverDate"), "%Y-%m-%d").year
        # TODO: esto se debería recoger en un nivel superior
        assert len(str(año)) == 4

        self.datos_carga_publicacion.set_año_publicacion(año)

    def cargar_fecha_publicacion(self):
        fecha = self.data[0].get("prism:coverDate")
        self.datos_carga_publicacion.set_fecha_publicacion(fecha)

    def cargar_doi(self):
        valor = self.data[0].get("prism:doi")
        identificador = DatosCargaIdentificadorPublicacion(valor=valor, tipo="doi")
        self.datos_carga_publicacion.add_identificador(identificador)

    def cargar_scopus(self):
        scopus: str = self.idScopus
        assert scopus.startswith("2-s2.0-")

        identificador = DatosCargaIdentificadorPublicacion(valor=scopus, tipo="scopus")
        self.datos_carga_publicacion.add_identificador(identificador)

    def cargar_identificadores(self):
        self.cargar_doi()
        self.cargar_scopus()

    def cargar_volumen(self):
        valor = self.data[0].get("prism:volumen")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="volumen", valor=valor)

    def cargar_numero(self):
        valor = self.data[0].get("article-number")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="numero", valor=valor)

    def cargar_pag_inicio_fin(self):
        rango = self.data[0].get("prism:pageRange")
        if rango is not None and "-" in rango:
            pags = rango.split("-")
            pag_inicio = pags[0].strip() if pags[0] else None  # Validar inicio
            pag_fin = (
                pags[1].strip() if len(pags) > 1 and pags[1] else None
            )  # Validar fin
            if pag_inicio:
                dato_inicio = DatosCargaDatoPublicacion(
                    tipo="pag_inicio", valor=pag_inicio
                )
            if pag_fin:
                dato_fin = DatosCargaDatoPublicacion(tipo="pag_fin", valor=pag_fin)

    def cargar_datos(self):
        if self.datos_carga_publicacion.es_tesis():
            return None
        self.cargar_volumen()
        self.cargar_numero()
        self.cargar_pag_inicio_fin()

    def cargar_issn(self):
        issn = self.data[0].get("prism:issn")
        if not issn:
            return None
        identificador = DatosCargaIdentificadorFuente(valor=issn, tipo="issn")
        self.datos_carga_publicacion.fuente.add_identificador(identificador)

    def cargar_eissn(self):
        eissn = self.data[0].get("prism:eIssn")
        if not eissn:
            return None
        identificador = DatosCargaIdentificadorFuente(valor=eissn, tipo="eissn")
        self.datos_carga_publicacion.fuente.add_identificador(identificador)

    def cargar_isbn(self):
        isbn = self.data[0].get("prism:isbn")
        if not isbn:
            return None
        identificador = DatosCargaIdentificadorFuente(valor=isbn, tipo="isbn")
        self.datos_carga_publicacion.fuente.add_identificador(identificador)

    def cargar_titulo_y_tipo(self):
        tipos_fuente = {
            "Journal": "Revista",
            "Conference proceeding": "Conference Proceeding",
            "Book series": "Book in series",
            "Book": "Libro",
            "Trade journal": "Revista",
            "Undefined": "Desconocido",
        }
        titulo = self.data[0].get("prism:publicationName")
        tipo_scopus = self.data[0].get("prism:aggregationType")
        tipo_fuente = tipos_fuente.get(tipo_scopus) or tipo_scopus
        # TODO: revisar si debe contemplarse en la carga
        # COLECCIÓN: tipo Libros que pertenecen a revista
        # if self.datos_carga_publicacion.tipo == "Libro" and tipo_fuente == "Revista":
        #     tipo = "Colección"
        self.datos_carga_publicacion.fuente.set_titulo(titulo)
        self.datos_carga_publicacion.fuente.set_tipo(tipo_fuente)

    def carga_editorial(self):
        # No viene en la llamada de Scopus
        pass

    def cargar_fuente(self):
        self.cargar_issn()
        self.cargar_eissn()
        self.cargar_isbn()
        self.cargar_titulo_y_tipo()
        # self.carga_editorial()
