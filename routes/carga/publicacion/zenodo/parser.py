from integration.apis.zenodo.zenodo import ZenodoAPI
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaAutor,
    DatosCargaDatoPublicacion,
    DatosCargaEditorial,
    DatosCargaIdentificadorPublicacion,
    DatosCargaIdentificadorFuente,
    DatosCargaIdentificadorAutor,
    DatosCargaAfiliacionesAutor,
    DatosCargaAfiliacionesAutor,
    DatosCargaFechaPublicacion,
    DatosCargaFuente,
    DatosCargaDatosFuente,
    DatosCargaFinanciacion,
    DatosCargaPublicacion,
)
from routes.carga.publicacion.parser import Parser
from datetime import datetime


class ZenodoParser(Parser):
    def __init__(self, data: dict) -> None:
        # Se inicializa la clase padre
        # La clase padre Parser tiene el atributo datos_carga_publicacion
        super().__init__()
        # Se definen los atributos de la clase
        self.data: dict = data
        self.carga()  # Con los datos recuperados, se rellena el objeto datos_carga_publicacion

    def set_fuente_datos(self):
        self.datos_carga_publicacion.set_fuente_datos("Zenodo")

    def cargar_titulo(self):
        titulo = self.data.get("metadata").get("title")
        if not titulo:
            return None
        self.datos_carga_publicacion.set_titulo(titulo)

    def cargar_titulo_alternativo(self):
        # TODO: Titulo Alt
        # for title in self.data["metadata"].get("additional_titles", []):
        #      self.datos_carga_publicacion.add_titulo_alternativo(title["title"])
        pass

    def cargar_tipo(self):
        tipo = self.data.get("metadata").get("resource_type").get("title").get("en")

        tipos = {
            # TODO: Aclarar tipos pubs - a resolver
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
        # TODO: Autores vacíos json - Controlar que no vengan los autores vacíos
        # Se extraen las afiliaciones de la publicacion
        cont = 1
        # TODO: hay autores con distintos roles en la misma publicacion (researcher, editor, project leader,supervisor )
        for autor in self.data.get("metadata").get("creators", []):
            # Se completa el Objeto DatosCargaAutor(Autor)

            firma = autor.get("person_or_org").get("name")
            orden = cont
            cont += 1
            # TODO: Aclarar rol autor - en este caso author, comprobar que nombre es en Prisma
            carga_autor = DatosCargaAutor(orden=orden, firma=firma, tipo=tipo)

            # Se completa el Objeto DatosCargaIdentificadorAutor(Identificador del Autor ORCID)
            if "identifiers" in autor.get("person_or_org"):
                for id in autor.get("person_or_org").get("identifiers"):
                    if id.get("scheme") == "orcid":
                        id_autor_orcid = DatosCargaIdentificadorAutor(
                            tipo="orcid", valor=id.get("identifier")
                        )
                        carga_autor.add_id(
                            id_autor_orcid
                        )  # Lo añadimos al objeto de Autor

            # TODO: Zenodo solo ofrece nombre afiliacion
            if "affiliations" in autor:
                for aff in autor.get("affiliations"):
                    afiliacion_autor = DatosCargaAfiliacionesAutor(
                        nombre=aff.get("name")
                    )
                    carga_autor.add_afiliacion(afiliacion=afiliacion_autor)

            self.datos_carga_publicacion.add_autor(carga_autor)

    def cargar_autores(self):
        self._cargar_autores(
            tipo="Autor/a",
            attr_name="author",
        )

    def cargar_editores(self):
        pass

    # TODO: Introducir editores - Revisar donde está en el JSON

    def cargar_directores(self):
        pass

    def _verificar_formato_fecha(self, fecha, formato):
        try:
            datetime.strptime(fecha, formato)
            return True
        except (ValueError, TypeError):
            return False

    def cargar_año_publicacion(self):
        formato1 = "%Y-%m-%d"
        formato2 = "%Y"

        metadata = self.data.get("metadata", {})
        fecha = metadata.get("publication_date")

        if not fecha:
            raise ValueError("No se encontró la fecha de publicación en los datos.")

        año = None
        if self._verificar_formato_fecha(fecha, formato1):
            año = datetime.strptime(fecha, formato1).year
        elif self._verificar_formato_fecha(fecha, formato2):
            año = datetime.strptime(fecha, formato2).year

        if not año or len(str(año)) != 4:
            raise ValueError(f"Formato de fecha inválido: {fecha}")

        self.datos_carga_publicacion.set_agno_publicacion(año)

    def cargar_fecha_publicacion(self):
        formato1 = "%Y-%m-%d"
        formato2 = "%Y"

        metadata = self.data.get("metadata", {})
        fecha = metadata.get("publication_date")

        if not fecha:
            raise ValueError("No se encontró la fecha de publicación en los datos.")

        año = None
        if self._verificar_formato_fecha(fecha, formato1):
            agno = str(datetime.strptime(fecha, formato1).year)
            mes = datetime.strptime(fecha, formato1).month
            mes = f"{mes:02d}"
            fecha_insercion = DatosCargaFechaPublicacion(
                tipo="publicación", agno=agno, mes=mes
            )
            self.datos_carga_publicacion.add_fechas_publicacion(fecha_insercion)
        elif self._verificar_formato_fecha(fecha, formato2):
            agno = str(datetime.strptime(fecha, formato2).year)
            fecha_insercion = DatosCargaFechaPublicacion(tipo="publicación", agno=agno)
            self.datos_carga_publicacion.add_fechas_publicacion(fecha_insercion)

    def cargar_identificadores(self):
        # Identificador ppal Zenodo
        identificador_zenodo = DatosCargaIdentificadorPublicacion(
            valor=self.data.get("id"), tipo="zenodo"
        )
        self.datos_carga_publicacion.add_identificador(identificador_zenodo)

        # Identificador doi Zenodo
        if "doi" in self.data.get("pids"):
            identificador_doi = DatosCargaIdentificadorPublicacion(
                valor=self.data.get("pids").get("doi").get("identifier"),
                tipo="doi",
            )
        self.datos_carga_publicacion.add_identificador(identificador_doi)

    # TODO: Vol en libro
    def cargar_volumen(self):
        if "journal:journal" in self.data.get("custom_fields"):
            valor = self.data.get("custom_fields").get("journal:journal").get("volume")
            if not valor:
                return None
            dato = DatosCargaDatoPublicacion(tipo="volumen", valor=valor)
            self.datos_carga_publicacion.add_dato(dato)

    def cargar_numero(self):
        if "journal:journal" in self.data.get("custom_fields"):
            valor = self.data.get("custom_fields").get("journal:journal").get("issue")
            if not valor:
                return None
            dato = DatosCargaDatoPublicacion(tipo="numero", valor=valor)
            self.datos_carga_publicacion.add_dato(dato)

    def cargar_numero_art(self):
        pass

    # TODO: No P.Ini y P.fin - viene el numero de paginas totales
    def cargar_pag_inicio_fin(self):
        pass

    def cargar_datos(self):
        if self.datos_carga_publicacion.es_tesis():
            return None
        self.cargar_volumen()
        self.cargar_numero()
        self.cargar_numero_art()
        # self.cargar_pag_inicio_fin()

    # TODO: id si es libro o electronico
    def cargar_ids_fuente(self):
        if "journal:journal" in self.data.get("custom_fields"):
            valor = self.data.get("custom_fields").get("journal:journal").get("issn")
            if valor:
                identificador = DatosCargaIdentificadorFuente(valor=valor, tipo="issn")
                self.datos_carga_publicacion.fuente.add_identificador(id)

        if "identifiers" in self.data.get("metadata"):
            for id in self.data.get("metadata").get("identifiers"):
                match id.get("scheme"):
                    case "isbn":
                        identificador = DatosCargaIdentificadorFuente(
                            valor=id.get("identifier"), tipo="isbn"
                        )
                        self.datos_carga_publicacion.fuente.add_identificador(id)

    def cargar_titulo_y_tipo(self):
        # TODO: Aclarar tipos de fuentes
        tipos_fuente = {
            "Journal": "Revista",
            "Conference proceeding": "Conference Proceeding",
            "Book series": "Book in series",
            "Book": "Libro",
            "Trade journal": "Revista",
            "Undefined": "Desconocido",
        }
        # Titulo y tipo
        if "journal:journal" in self.data.get("custom_fields"):
            self.datos_carga_publicacion.fuente.set_tipo("Revista")

            titulo = self.data.get("custom_fields").get("journal:journal").get("title")
            if not titulo:
                return None
            self.datos_carga_publicacion.fuente.set_titulo(titulo)
        # TODO: book hacerlo para book

    # TODO: no viene info de la editorial
    def carga_editorial(self):
        pass

    def cargar_fuente(self):
        self.cargar_ids_fuente()
        self.cargar_titulo_y_tipo()
        # self.carga_editorial()

    def cargar_financiacion(self):
        pass

    def carga_acceso_abierto(self):
        pass
