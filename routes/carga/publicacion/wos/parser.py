from integration.apis.clarivate.wos.wos_api import WosAPI
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
)
from routes.carga.publicacion.parser import Parser
from datetime import datetime


class WosParser(Parser):
    def __init__(self, data: dict) -> None:
        # Se inicializa la clase padre
        # La clase padre Parser tiene el atributo datos_carga_publicacion
        super().__init__()
        # Se definen los atributos de la clase
        self.data: dict = data
        self.carga()  # Con los datos recuperados, se rellena el objeto datos_carga_publicacion

    def set_fuente_datos(self):
        self.datos_carga_publicacion.set_fuente_datos("WOS")

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
        pass

    def cargar_tipo(self):
        tipo = self.data["static_data"]["summary"]["doctypes"]["doctype"]

        if self.data["static_data"]["summary"]["doctypes"]["count"] > 1:
            if "Article" in tipo:
                tipo = "Article"
            else:
                valor = tipo[0]

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

        for autor in self.data["static_data"]["summary"]["names"]["name"]:
            # Se completa el Objeto DatosCargaAutor(Autor)
            firma = autor["display_name"]
            orden = autor["seq_no"]
            # TODO: Aclarar rol autor - en este caso author, comprobar que nombre es en Prisma
            carga_autor = DatosCargaAutor(orden=orden, firma=firma, tipo=tipo)

            # Se completa el Objeto DatosCargaIdentificadorAutor(Identificador del Autor ResearcherId)
            if "r_id" in autor:
                id_autor_rid = DatosCargaIdentificadorAutor(
                    tipo="researcherid", valor=autor.get("r_id")
                )
                carga_autor.add_id(id_autor_rid)  # Lo añadimos al objeto de Autor

            # Se completa el Objeto DatosCargaIdentificadorAutor(Identificador del Autor ORCID)
            if "orcid_id" in autor:
                id_autor_orcid = DatosCargaIdentificadorAutor(
                    tipo="orcid", valor=autor.get("orcid_id")
                )
                carga_autor.add_id(id_autor_orcid)  # Lo añadimos al objeto de Autor
            # Se completa las el Objeto DatosCargaAfiliacion(Afiliaciones del Autor)

            # Esto se hace porque a veces addresses es un diccionario y a veces una lista
            addresses = self.data["static_data"]["fullrecord_metadata"][
                "addresses"
            ].get("address_name", [])

            if isinstance(addresses, dict):
                addresses = [addresses]

            for aff in addresses:
                author_in_aff = False

                if "name" in aff:
                    name = aff["names"].get("name", [])

                    # afiliaciones con 1 autor
                    if aff["names"]["count"] == 1:
                        author_in_aff = name["display_name"] == firma
                    else:  # afiliaciones con varios autores
                        author_in_aff = any(
                            aff_name_author["display_name"] == firma
                            for aff_name_author in name
                        )

                    if author_in_aff:
                        pais_aff = aff["address_spec"]["country"]

                        for aff_org in aff["address_spec"]["organizations"].get(
                            "organization"
                        ):
                            if aff_org["pref"] == "Y":
                                nombre_aff = aff_org["content"]
                                ror_aff = aff_org.get("ror_id", None)

                                afiliacion_autor = DatosCargaAfiliacionesAutor(
                                    nombre=nombre_aff, pais=pais_aff, ror_id=ror_aff
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

    def cargar_año_publicacion(self):
        agno = str(
            datetime.strptime(
                self.data["static_data"]["summary"]["pub_info"]["sortdate"], "%Y-%m-%d"
            ).year
        )
        if len(str(agno)) != 4:
            raise TypeError("El año no tiene el formato correcto")

        self.datos_carga_publicacion.set_agno_publicacion(agno)

    def cargar_fecha_publicacion(self):
        # Fecha pub
        fecha = self.data["static_data"]["summary"]["pub_info"]["sortdate"]
        agno = str(datetime.strptime(fecha, "%Y-%m-%d").year)
        mes = datetime.strptime(fecha, "%Y-%m-%d").month
        mes = f"{mes:02d}"
        if len(str(agno)) != 4 or len(str(mes)) != 2:
            raise TypeError("El mes o el año no tiene el formato correcto")
        fecha_insercion = DatosCargaFechaPublicacion(
            tipo="publicación", agno=agno, mes=mes
        )
        self.datos_carga_publicacion.add_fechas_publicacion(fecha_insercion)

    def cargar_identificadores(self):
        # Identificador ppal WOS
        identificador_wos = DatosCargaIdentificadorPublicacion(
            valor=self.data.get("UID"), tipo="wos"
        )
        self.datos_carga_publicacion.add_identificador(identificador_wos)

        identifier = self.data["dynamic_data"]["cluster_related"]["identifiers"].get(
            "identifier", []
        )
        # Si es un diccionario, lo convertimos en una lista para evitar errores
        if isinstance(identifier, dict):
            identifier = [identifier]

        for id in identifier:
            match id["type"]:
                case "doi":
                    identificador = DatosCargaIdentificadorPublicacion(
                        valor=id["value"], tipo="doi"
                    )
                    self.datos_carga_publicacion.add_identificador(identificador)
                case "pmid":
                    identificador = DatosCargaIdentificadorPublicacion(
                        valor=id["value"], tipo="pubmed"
                    )
                    self.datos_carga_publicacion.add_identificador(identificador)

    def cargar_volumen(self):
        valor = self.data["static_data"]["summary"]["pub_info"].get("vol")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="volumen", valor=valor)
        self.datos_carga_publicacion.add_dato(dato)

    # TODO: Revisar bien la diferencia entre número y número de artículo
    def cargar_numero(self):
        valor = self.data["static_data"]["summary"]["pub_info"].get("issue")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="numero", valor=valor)
        self.datos_carga_publicacion.add_dato(dato)

    def cargar_numero_art(self):
        identifier = self.data["dynamic_data"]["cluster_related"]["identifiers"].get(
            "identifier", []
        )
        # Si es un diccionario, lo convertimos en una lista para evitar errores
        if isinstance(identifier, dict):
            identifier = [identifier]

        for id in identifier:
            match id["type"]:
                # Esto es una excepcion, pero se guarda en IDs en el JSON
                case "art_no":
                    identificador = DatosCargaIdentificadorPublicacion(
                        valor=id["value"], tipo="num_articulo"
                    )
                    self.datos_carga_publicacion.add_dato(identificador)

    # TODO: No P.Ini y P.fin - viene el numero de paginas totales
    def cargar_pag_inicio_fin(self):
        pass

    def cargar_datos(self):
        if self.datos_carga_publicacion.es_tesis():
            return None
        self.cargar_volumen()
        self.cargar_numero()
        self.cargar_numero_art()
        self.cargar_pag_inicio_fin()

    def cargar_ids_fuente(self):
        identifier = self.data["dynamic_data"]["cluster_related"]["identifiers"].get(
            "identifier", []
        )
        # Si es un diccionario, lo convertimos en una lista para evitar errores
        if isinstance(identifier, dict):
            identifier = [identifier]

        for id in identifier:
            match id["type"]:
                case "issn":
                    identificador = DatosCargaIdentificadorFuente(
                        valor=id["value"], tipo="issn"
                    )
                    self.datos_carga_publicacion.fuente.add_identificador(identificador)
                case "eissn":
                    identificador = DatosCargaIdentificadorFuente(
                        valor=id["value"], tipo="eissn"
                    )
                    self.datos_carga_publicacion.fuente.add_identificador(identificador)
                case "isbn":
                    identificador = DatosCargaIdentificadorFuente(
                        valor=id["value"], tipo="isbn"
                    )
                    self.datos_carga_publicacion.fuente.add_identificador(identificador)
                case "eisbn":
                    identificador = DatosCargaIdentificadorFuente(
                        valor=id["value"], tipo="eisbn"
                    )
                    self.datos_carga_publicacion.fuente.add_identificador(identificador)

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
        # Titulo
        titulo_dict = self.data["static_data"]["summary"]["titles"]
        titulo = next(
            (
                element.get("content")
                for element in titulo_dict["title"]
                if element.get("type") == "source"
            ),
            None,
        )

        tipo_wos = self.data["static_data"]["summary"]["pub_info"]["pubtype"]
        tipo_fuente = tipos_fuente.get(tipo_wos) or tipo_wos
        self.datos_carga_publicacion.fuente.set_titulo(titulo)
        self.datos_carga_publicacion.fuente.set_tipo(tipo_fuente)

    def carga_editorial(self):
        # TODO: 2 Editoriales - puede darse el caso ?
        if (
            self.data["static_data"]["summary"]["publishers"]["publisher"]["names"][
                "count"
            ]
            == 1
        ):
            editorial = DatosCargaEditorial(
                self.data["static_data"]["summary"]["publishers"]["publisher"]["names"][
                    "name"
                ]["display_name"]
            )
            # TODO: pais Editorial
            self.datos_carga_publicacion.fuente.add_editorial(editorial)
        else:  # afiliaciones con varios autores
            for ed in self.data["static_data"]["summary"]["publishers"]["publisher"][
                "names"
            ].get("name", []):
                editorial = DatosCargaEditorial(ed["display_name"])
                self.datos_carga_publicacion.fuente.add_editorial(editorial)

    def cargar_fuente(self):
        self.cargar_ids_fuente()
        self.cargar_titulo_y_tipo()
        self.carga_editorial()

    def cargar_financiacion(self):
        if "fund_ack" in self.data["static_data"].get(
            "fullrecord_metadata"
        ):  # Se comprueba que exista info de financiacion
            for agencia_obj in self.data["static_data"]["fullrecord_metadata"][
                "fund_ack"
            ]["grants"].get("grant", []):
                if (
                    "grant_ids" in agencia_obj and "grant_agency" in agencia_obj
                ):  # Se comprueba que para cada entrada haya agencia y proyecto
                    agencia = agencia_obj.get("grant_agency")
                    n_proyectos = agencia_obj["grant_ids"].get("count", [])
                    if n_proyectos > 1:
                        proyectos = agencia_obj["grant_ids"].get("grant_id", [])
                        for proyecto_obj in proyectos:
                            financiacion = DatosCargaFinanciacion(
                                entidad_financiadora=agencia, proyecto=proyecto_obj
                            )
                            self.datos_carga_publicacion.add_financiacion(financiacion)
                    elif n_proyectos == 1:
                        proyecto = agencia_obj["grant_ids"].get("grant_id")
                        financiacion = DatosCargaFinanciacion(
                            entidad_financiadora=agencia, proyecto=proyecto
                        )

    def carga_acceso_abierto(self):
        pass
