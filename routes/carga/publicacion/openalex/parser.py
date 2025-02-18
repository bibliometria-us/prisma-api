from integration.apis.openalex.openalex import OpenalexAPI
from routes.carga.publicacion.datos_carga_publicacion import (
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
    DatosCargaAccesoAbierto,
)
from routes.carga.publicacion.parser import Parser
from datetime import datetime
import routes.carga.publicacion.openalex.country_codes as country_codes


class OpenalexParser(Parser):
    def __init__(self, data: dict) -> None:
        # Se inicializa la clase padre
        # La clase padre Parser tiene el atributo datos_carga_publicacion
        super().__init__()
        # Se definen los atributos de la clase
        self.data: dict = data
        self.carga()  # Con los datos recuperados, se rellena el objeto datos_carga_publicacion

    def set_fuente_datos(self):
        self.datos_carga_publicacion.set_fuente_datos("Openalex")

    def api_request(self):
        pass

    def cargar_titulo(self):
        titulo = self.data.get("title")
        self.datos_carga_publicacion.set_titulo(titulo)

    def cargar_titulo_alternativo(self):
        # TODO: Titulo Alt
        # for title in self.data["metadata"].get("additional_titles", []):
        #      self.datos_carga_publicacion.add_titulo_alternativo(title["title"])
        pass

    def cargar_tipo(self):
        tipo = self.data.get("type")

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
        cont = 1
        for autor in self.data.get("authorships", []):
            # Se completa el Objeto DatosCargaAutor(Autor)
            identificadores = {}
            firma = None
            autor_info = autor.get("author", [])
            if "display_name" in autor_info:
                firma = autor_info.get("display_name")
            if "id" in autor_info:
                openalex_id = autor_info.get("id").split("/")[-1]
                identificadores["openalex_id"] = openalex_id
            if "orcid" in autor_info:
                if autor_info.get("orcid") is not None:
                    orcid = autor_info.get("orcid").split("/")[-1]
                    identificadores["orcid"] = orcid

            orden = cont

            carga_autor = DatosCargaAutor(orden=orden, firma=firma, tipo=tipo)
            cont += 1

            tipo_identificadores = {"orcid": "orcid", "openalex_id": "openalex_id"}

            for key_id, value_id in identificadores.items():
                if key_id in tipo_identificadores:
                    tipo = tipo_identificadores[key_id]
                    id_autor = DatosCargaIdentificadorAutor(tipo=tipo, valor=value_id)
                    carga_autor.add_id(id_autor)

            for aff in autor.get("institutions", []):
                nombre = aff.get("display_name")
                if aff.get("ror") is not None:
                    ror_id = aff.get("ror").split("/")[-1]
                else:
                    ror_id = None
                if (
                    aff.get("country_code") is not None
                    and aff.get("country_code") in country_codes.country_codes
                ):
                    pais = country_codes.country_codes[aff.get("country_code")]
                else:
                    pais = None
                afiliacion_autor = DatosCargaAfiliacionesAutor(
                    nombre=nombre, pais=pais, ror_id=ror_id
                )
                carga_autor.add_afiliacion(afiliacion=afiliacion_autor)
            # TODO: correspondig author, buscar un ejemplo
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
            # Intenta convertir la fecha con el formato especificado
            datetime.strptime(fecha, formato)
            return True
        except ValueError:
            # Si hay un error, no coincide con el formato
            return False

    def cargar_año_publicacion(self):
        formato = "%Y-%m-%d"
        if not self._verificar_formato_fecha(
            self.data.get("publication_date"), formato
        ):
            return None
        fecha = datetime.strptime(self.data.get("publication_date"), "%Y-%m-%d")
        agno = str(fecha.year)
        if len(str(agno)) != 4:
            raise TypeError("El año no tiene el formato correcto")
        self.datos_carga_publicacion.set_agno_publicacion(agno)

    def cargar_fecha_publicacion(self):
        formato = "%Y-%m-%d"
        if not self._verificar_formato_fecha(
            self.data.get("publication_date"), formato
        ):
            return None
        fecha = datetime.strptime(self.data.get("publication_date"), "%Y-%m-%d")
        agno = str(fecha.year)
        mes = fecha.month
        mes = f"{mes:02d}"
        if len(agno) != 4 or len(mes) != 2:
            raise TypeError("El mes o el año no tiene el formato correcto")
        fecha_insercion = DatosCargaFechaPublicacion(
            tipo="publicación", agno=agno, mes=mes
        )
        self.datos_carga_publicacion.add_fechas_publicacion(fecha_insercion)

    def cargar_identificadores(self):
        # Identificador ppal Openalex
        identificador_openalex = DatosCargaIdentificadorPublicacion(
            valor=self.data.get("id").split("/")[-1], tipo="openalex"
        )
        self.datos_carga_publicacion.add_identificador(identificador_openalex)

        # Identificador doi Openalex
        if "doi" in self.data:
            identificador_doi = DatosCargaIdentificadorPublicacion(
                valor=self.data.get("doi").split("/")[-1], tipo="doi"
            )
        self.datos_carga_publicacion.add_identificador(identificador_doi)

    def cargar_volumen(self):
        if self.data.get("biblio").get("volume"):
            valor = self.data.get("biblio").get("volume")
            if not valor:
                return None
            dato = DatosCargaDatoPublicacion(tipo="volumen", valor=valor)
            self.datos_carga_publicacion.add_dato(dato)

    def cargar_numero(self):
        if self.data.get("biblio").get("issue"):
            valor = self.data.get("biblio").get("issue")
            if not valor:
                return None
            dato = DatosCargaDatoPublicacion(tipo="numero", valor=valor)
            self.datos_carga_publicacion.add_dato(dato)

    def cargar_numero_art(self):
        pass

    def cargar_pag_inicio_fin(self):
        if self.data.get("biblio").get("first_page"):
            valor = self.data.get("biblio").get("first_page")
            if not valor:
                return None
            dato = DatosCargaDatoPublicacion(tipo="pag_inicio", valor=valor)
            self.datos_carga_publicacion.add_dato(dato)

        if self.data.get("biblio").get("last_page"):
            valor = self.data.get("biblio").get("last_page")
            if not valor:
                return None
            dato = DatosCargaDatoPublicacion(tipo="pag_fin", valor=valor)
            self.datos_carga_publicacion.add_dato(dato)

    def cargar_datos(self):
        if self.datos_carga_publicacion.es_tesis():
            return None
        self.cargar_volumen()
        self.cargar_numero()
        self.cargar_numero_art()
        self.cargar_pag_inicio_fin()

    def cargar_ids_fuente(self):
        #  TODO: REVISAR
        if len(self.data.get("primary_location").get("source").get("issn")) > 0:
            for id in self.data.get("primary_location").get("source").get("issn"):
                identificador = DatosCargaIdentificadorFuente(valor=id, tipo="issn")
                self.datos_carga_publicacion.fuente.add_identificador(id)
        #  TODO: REVISAR ISBN

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
        if self.data.get("primary_location").get("source"):
            tipo = self.data.get("primary_location").get("source").get("type")
            if tipo in tipos_fuente:
                self.datos_carga_publicacion.fuente.set_tipo(tipos_fuente[tipo])
            else:
                self.datos_carga_publicacion.fuente.set_tipo(tipo)  # Tipo no conocido

            titulo = self.data.get("primary_location").get("source").get("display_name")
            if not titulo:
                return None
            self.datos_carga_publicacion.fuente.set_titulo(titulo)

    # TODO: revisar donde viene la editorial
    def carga_editorial(self):
        pass

    def cargar_fuente(self):
        self.cargar_ids_fuente()
        self.cargar_titulo_y_tipo()
        self.carga_editorial()

    def carga_acceso_abierto(self):
        if self.data.get("open_access").get("is_oa"):
            valor = self.data.get("open_access").get("oa_status")
            origen = "openalex"
            if not valor:
                return None
            dato = DatosCargaAccesoAbierto(valor=valor, origen=origen)
            self.datos_carga_publicacion.add_acceso_abierto(dato)

    def cargar_financiacion(self):
        # TODO: revisar donde viene la financiacion (grant)
        for grant in self.data.get("grants", []):
            entidad = grant.get("funder_display_name")
            proyecto = grant.get("award_id")
            financiacion = DatosCargaFinanciacion(
                entidad_financiadora=entidad, proyecto=proyecto
            )
            self.datos_carga_publicacion.add_financiacion(financiacion)
