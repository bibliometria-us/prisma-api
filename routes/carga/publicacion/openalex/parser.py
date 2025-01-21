from integration.apis.openalex.openalex import OpenalexAPI
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaAutor,
    DatosCargaDatoPublicacion,
    DatosCargaEditorial,
    DatosCargaIdentificadorPublicacion,
    DatosCargaIdentificadorFuente,
    DatosCargaIdentificadorAutor,
    DatosCargaAfiliacionesAutor,
)
from routes.carga.publicacion.parser import Parser
from datetime import datetime


class OpenalexParser(Parser):
    def __init__(self, idOpenalex: str) -> None:
        # Se inicializa la clase padre
        # La clase padre Parser tiene el atributo datos_carga_publicacion
        super().__init__()
        # Se definen los atributos de la clase
        self.idOpenalex = idOpenalex
        self.data: dict = None
        self.api_request()  # Se hace la petición de Openalex
        self.carga()  # Con los datos recuperados, se rellena el objeto datos_carga_publicacion

    def set_fuente_datos(self):
        self.datos_carga_publicacion.set_fuente_datos("Openalex")

    def api_request(self):
        api = OpenalexAPI()
        response = api.search_by_doi(self.idOpenalex)

        self.data = response

    def cargar_titulo(self):
        titulo = self.data["0"]["metadata"].get("title")
        self.datos_carga_publicacion.set_titulo(titulo)

    def cargar_titulo_alternativo(self):
        # TODO: Titulo Alt
        # for title in self.data["metadata"].get("additional_titles", []):
        #      self.datos_carga_publicacion.add_titulo_alternativo(title["title"])
        pass

    def cargar_tipo(self):
        tipo = self.data["0"]["metadata"]["resource_type"]["title"].get("en")

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
        for autor in self.data["metadata"].get("creators", []):
            # Se completa el Objeto DatosCargaAutor(Autor)

            firma = autor["person_or_org"]["name"]
            orden = cont
            cont += 1
            # TODO: Aclarar rol autor - en este caso author, comprobar que nombre es en Prisma
            carga_autor = DatosCargaAutor(orden=orden, firma=firma, tipo=tipo)

            # Se completa el Objeto DatosCargaIdentificadorAutor(Identificador del Autor ORCID)
            if "identifiers" in autor["person_or_org"]:
                for id in autor["person_or_org"]["identifiers"]:
                    if id["scheme"] == "orcid":
                        id_autor_orcid = DatosCargaIdentificadorAutor(
                            tipo="orcid", valor=id.get("identifier")
                        )
                        carga_autor.add_id(
                            id_autor_orcid
                        )  # Lo añadimos al objeto de Autor

            # TODO: Zenono solo ofrece nombre afiliacion
            if "affiliations" in autor["person_or_org"]:
                for aff in autor["person_or_org"]["affiliations"]:
                    afiliacion_autor = DatosCargaAfiliacionesAutor(nombre=aff["name"])
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

    def _verificar_formato_fecha(fecha, formato):
        try:
            # Intenta convertir la fecha con el formato especificado
            datetime.strptime(fecha, formato)
            return True
        except ValueError:
            # Si hay un error, no coincide con el formato
            return False

    def cargar_año_publicacion(self):
        formato1 = "%Y-%m-%d"
        formato2 = "%Y"
        fecha = self.data["0"]["metadata"].get("publication_date")
        año = None
        if self._verificar_formato_fecha(fecha, formato1):
            año = datetime.strptime(fecha, formato1).year
        elif self._verificar_formato_fecha(fecha, formato2):
            año = datetime.strptime(fecha, formato2).year
        # TODO: Control Excep - (mejos raise que asserts) esto se debería recoger en un nivel superior (de hecho se comprueba 2 veces arriba)
        assert len(str(año)) == 4
        self.datos_carga_publicacion.set_año_publicacion(año)

    def cargar_mes_publicacion(self):
        formato1 = "%Y-%m-%d"
        fecha = self.data["0"]["metadata"].get("publication_date")
        mes = None
        if not self._verificar_formato_fecha(fecha, formato1):
            return None
        año = datetime.strptime(fecha, formato1).month
        # TODO: Control Excep - esto se debería recoger en un nivel superior (de hecho se comprueba 2 veces arriba)
        mes_formateado = f"{mes:02d}"  # Usando f-string para forzar 2 dígitos
        assert len(mes_formateado) == 2  # TODO: formato mes 1d o 2d
        self.datos_carga_publicacion.set_mes_publicacion(str(mes))

    def cargar_fecha_publicacion(self):
        formato1 = "%Y-%m-%d"
        fecha = self.data["0"]["metadata"].get("publication_date")
        if not self._verificar_formato_fecha(fecha, formato1):
            return None
        self.datos_carga_publicacion.set_fecha_publicacion(fecha)

    def cargar_identificadores(self):
        # Identificador ppal Zenodo
        identificador_zenodo = DatosCargaIdentificadorPublicacion(
            valor=self.data["0"]["id"], tipo="zenodo"
        )
        self.datos_carga_publicacion.add_identificador(identificador_zenodo)

        # Identificador doi Zenodo
        if "doi" in self.data["0"]["pids"]:
            identificador_doi = DatosCargaIdentificadorPublicacion(
                valor=self.data["0"]["pids"]["doi"].get("identifier"), tipo="doi"
            )
        self.datos_carga_publicacion.add_identificador(identificador_doi)

    # TODO: Vol en libro
    def cargar_volumen(self):
        if "journal:journal" in self.data["0"].get("custom_fields"):
            valor = self.data["0"]["custom_fields"]["journal:journal"].get("volume")
            if not valor:
                return None
            dato = DatosCargaDatoPublicacion(tipo="volumen", valor=valor)
            self.datos_carga_publicacion.add_dato(dato)

    def cargar_numero(self):
        if "journal:journal" in self.data["0"].get("custom_fields"):
            valor = self.data["0"]["custom_fields"]["journal:journal"].get("issue")
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
        if "journal:journal" in self.data["0"].get("custom_fields"):
            valor = self.data["0"]["custom_fields"]["journal:journal"].get("issn")
            if valor:
                identificador = DatosCargaIdentificadorFuente(valor=valor, tipo="issn")
                self.datos_carga_publicacion.fuente.add_identificador(id)

        if "identifiers" in self.data["0"].get("metadata"):
            for id in self.data["0"]["metadata"].get("identifiers"):
                match id["scheme"]:
                    case "isbn":
                        identificador = DatosCargaIdentificadorFuente(
                            valor=id["identifier"], tipo="isbn"
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
        if "journal:journal" in self.data["0"].get("custom_fields"):
            self.datos_carga_publicacion.fuente.set_tipo("Revista")

            titulo = self.data["0"]["custom_fields"]["journal:journal"].get("title")
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
