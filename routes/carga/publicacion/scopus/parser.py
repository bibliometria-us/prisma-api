from integration.apis.elsevier.scopus_search.scopus_search import ScopusSearch
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaAccesoAbierto,
    DatosCargaAutor,
    DatosCargaDatoPublicacion,
    DatosCargaEditorial,
    DatosCargaIdentificadorPublicacion,
    DatosCargaIdentificadorFuente,
    DatosCargaIdentificadorAutor,
    DatosCargaAfiliacionesAutor,
    DatosCargaFechaPublicacion,
    DatosCargaFuente,
    DatosCargaDatosFuente,
    DatosCargaFinanciacion,
    DatosCargaPublicacion,
)
from routes.carga.publicacion.parser import Parser
from datetime import datetime


class ScopusParser(Parser):
    def __init__(self, data: dict) -> None:
        # Se inicializa la clase padre
        # La clase padre Parser tiene el atributo datos_carga_publicacion
        super().__init__()
        # Se definen los atributos de la clase
        self.data: dict = data
        self.carga()  # Con los datos recuperados, se rellena el objeto datos_carga_publicacion

    def set_fuente_datos(self):
        self.datos_carga_publicacion.set_fuente_datos("Scopus")

    def api_request(self):
        api = ScopusSearch()
        response = api.get_publicaciones_por_id(self.idScopus)

        self.data = response

    def cargar_titulo(self):
        titulo = self.data.get("dc:title")
        self.datos_carga_publicacion.set_titulo(titulo)

    def cargar_titulo_alternativo(self):
        # Scopus no devuelve un título alternativo
        pass

    def cargar_tipo(self):
        tipo = self.data.get("subtype")

        # TODO: Aclarar los tipos de publicación
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
        # TODO: Autores vacios scopus - Controlar que no vengan los autores vacíos
        # Se extraen las TODAS las afiliaciones de la publicacion
        afiliaciones_publicacion = dict()
        for aff_pub in self.data.get("affiliation", []):
            afiliaciones_publicacion[aff_pub["afid"]] = {
                "nombre": aff_pub.get("affilname"),
                "pais": aff_pub.get("affiliation-country"),
                "ciudad": aff_pub.get("affiliation-city"),
            }

        for autor in self.data.get(attr_name, []):
            # Se completa el Objeto DatosCargaAutor(Autor)
            firma = autor.get("authname")
            orden = int(autor.get("@seq"))
            carga_autor = DatosCargaAutor(orden=orden, firma=firma, tipo=tipo)

            # Se completa el Objeto DatosCargaIdentificadorAutor(Identificador del Autor)
            tipo_id = "scopus"
            valor_id = autor["authid"]
            id_autor = DatosCargaIdentificadorAutor(tipo=tipo_id, valor=valor_id)
            carga_autor.add_id(id_autor)  # Lo añadimos al objeto de Autor

            # Se completa las el Objeto DatosCargaAfiliacionesAutor(Afiliaciones del Autor)
            for aff in autor.get("afid", []):
                if_aff = aff.get("$")
                nombre_aff = afiliaciones_publicacion[if_aff].get("nombre")
                pais_aff = afiliaciones_publicacion[if_aff].get("pais")
                ciudad_aff = afiliaciones_publicacion[if_aff].get("ciudad")
                afiliacion_autor = DatosCargaAfiliacionesAutor(
                    nombre=nombre_aff, pais=pais_aff, ciudad=ciudad_aff, ror_id=None
                )
                carga_autor.add_afiliacion(afiliacion_autor)
                # TODO: difieren aff scopus y wos: que pasa cuando las afiliaciones no son iguales en distintas fuentes

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

    # cargar fecha en fechas_publicion (usar metodo general)
    def cargar_año_publicacion(self):
        año = str(datetime.strptime(self.data.get("prism:coverDate"), "%Y-%m-%d").year)
        # TODO: Control Excep - esto se debería recoger en un nivel superior
        if len(año) != 4:
            raise TypeError("El año no tiene el formato correcto")

        self.datos_carga_publicacion.set_agno_publicacion(año)

    def cargar_mes_publicacion(self):
        # Implement the method to load the publication month
        pass

    def cargar_fecha_publicacion(self):
        # Fecha pub
        fecha = self.data.get("prism:coverDate")
        agno = datetime.strptime(fecha, "%Y-%m-%d").year
        str_agno = str(agno)
        mes = datetime.strptime(fecha, "%Y-%m-%d").month
        str_mes = f"{mes:02d}"
        if len(str_agno) != 4 or len(str_mes) != 2:
            raise TypeError("El mes o el año no tiene el formato correcto")
        fecha_insercion = DatosCargaFechaPublicacion(
            tipo="publicacion", agno=agno, mes=mes
        )
        self.datos_carga_publicacion.add_fechas_publicacion(fecha_insercion)
        # # Fecha early access
        # fecha_ea = self.data.get("prism:cov-")
        # agno_ea = datetime.strptime(agno_ea, "%Y-%m-%d").year
        # mes_ea = datetime.strptime(mes_ea, "%Y-%m-%d").month
        # fecha_early_access = DatosCargaFechaPublicacion(
        #     tipo="early_access", agno=agno_ea, mes=mes_ea
        # )
        # self.datos_carga_publicacion.add_fechas_publicacion(fecha_early_access)

    def cargar_doi(self):
        valor = self.data.get("prism:doi")
        identificador = DatosCargaIdentificadorPublicacion(valor=valor, tipo="doi")
        if valor:
            self.datos_carga_publicacion.add_identificador(identificador)

    def cargar_scopus(self):
        valor = self.data.get("eid")
        identificador = DatosCargaIdentificadorPublicacion(valor=valor, tipo="scopus")
        if valor:
            self.datos_carga_publicacion.add_identificador(identificador)

    def cargar_identificadores(self):
        self.cargar_doi()
        self.cargar_scopus()

    def cargar_volumen(self):
        valor = self.data.get("prism:volume")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="volumen", valor=valor)
        self.datos_carga_publicacion.add_dato(dato)

    def cargar_numero(self):
        valor = self.data.get("article-number")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="numero", valor=valor)
        self.datos_carga_publicacion.add_dato(dato)

    # TODO: ver si procede
    def cargar_numero_issue(self):
        valor = self.data.get("prism:issueIdentifier")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="num_articulo", valor=valor)
        self.datos_carga_publicacion.add_dato(dato)

    def cargar_pag_inicio_fin(self):
        rango = self.data.get("prism:pageRange")
        if rango is not None and "-" in rango:
            pags = rango.split("-")
            pag_inicio = pags[0].strip() if pags else None  # Validar inicio
            pag_fin = (
                pags[1].strip() if len(pags) > 1 and pags[1] else None
            )  # Validar fin
            if pag_inicio:
                dato_inicio = DatosCargaDatoPublicacion(
                    tipo="pag_inicio", valor=pag_inicio
                )
                self.datos_carga_publicacion.add_dato(dato_inicio)
            if pag_fin:
                dato_fin = DatosCargaDatoPublicacion(tipo="pag_fin", valor=pag_fin)
                self.datos_carga_publicacion.add_dato(dato_fin)

    def cargar_datos(self):
        if self.datos_carga_publicacion.es_tesis():
            return None
        self.cargar_volumen()
        self.cargar_numero()
        self.cargar_numero_issue()
        self.cargar_pag_inicio_fin()

    def cargar_issn(self):
        issn = self.data.get("prism:issn")
        if not issn:
            return None
        identificador = DatosCargaIdentificadorFuente(valor=issn, tipo="issn")
        self.datos_carga_publicacion.fuente.add_identificador(identificador)

    def cargar_eissn(self):
        eissn = self.data.get("prism:eIssn")
        if not eissn:
            return None
        identificador = DatosCargaIdentificadorFuente(valor=eissn, tipo="eissn")
        self.datos_carga_publicacion.fuente.add_identificador(identificador)

    def cargar_isbn(self):
        isbn_data = self.data.get("prism:isbn")
        if not isbn_data:
            return None

        isbns = list(isbn.strip() for isbn in isbn_data[0]["$"][1:-1].split(","))
        for isbn in isbns:
            identificador = DatosCargaIdentificadorFuente(valor=isbn, tipo="isbn")
            self.datos_carga_publicacion.fuente.add_identificador(identificador)

    def cargar_eisbn(self):
        pass

    def cargar_edicion_fuente(self):
        valor = self.data.get("prism:issueIdentifier")
        if not valor:
            return None
        dato = DatosCargaDatosFuente(valor=valor, tipo="edición")
        self.datos_carga_publicacion.fuente.add_dato(dato)

    def cargar_titulo_y_tipo(self):
        # TODO: Aclarar los tipos de fuentes
        tipos_fuente = {
            "Journal": "Revista",
            "Conference proceeding": "Conference Proceeding",
            "Book series": "Book in series",
            "Book": "Libro",
            "Trade journal": "Revista",
            "Undefined": "Desconocido",
        }
        titulo = self.data.get("prism:publicationName")
        tipo_scopus = self.data.get("prism:aggregationType")
        tipo_fuente = tipos_fuente.get(tipo_scopus) or tipo_scopus

        self.datos_carga_publicacion.fuente.set_titulo(titulo)
        self.datos_carga_publicacion.fuente.set_tipo(tipo_fuente)

    def carga_editorial(self):
        # No viene en la llamada de Scopus
        pass

    def carga_acceso_abierto(self):
        free_to_read_label = self.data.get("freetoreadLabel", {}).get("value", {})
        if any(value.get("$") == "Green" for value in free_to_read_label):
            acceso_abierto = DatosCargaAccesoAbierto(origen="scopus", valor="green")
            self.datos_carga_publicacion.add_acceso_abierto(acceso_abierto)

        if any(value.get("$") == "Gold" for value in free_to_read_label):
            acceso_abierto = DatosCargaAccesoAbierto(origen="scopus", valor="gold")
            self.datos_carga_publicacion.add_acceso_abierto(acceso_abierto)

        if any(value.get("$") == "Bronze" for value in free_to_read_label):
            acceso_abierto = DatosCargaAccesoAbierto(origen="scopus", valor="bronze")
            self.datos_carga_publicacion.add_acceso_abierto(acceso_abierto)

        if any(value.get("$") == "Hybrid Gold" for value in free_to_read_label):
            acceso_abierto = DatosCargaAccesoAbierto(
                origen="scopus", valor="hybrid_gold"
            )
            self.datos_carga_publicacion.add_acceso_abierto(acceso_abierto)

        pass

    def cargar_fuente(self):
        self.cargar_issn()
        self.cargar_eissn()
        self.cargar_isbn()
        self.cargar_eisbn()
        self.cargar_titulo_y_tipo()
        self.carga_editorial()
        self.carga_acceso_abierto()
        self.cargar_fecha_publicacion()
        # self.cargar_edicion_fuente()

    def cargar_financiacion(self):
        # TODO: la API de Scopus no deuelva la información correctamente
        pass
