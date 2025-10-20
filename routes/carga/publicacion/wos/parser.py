from integration.apis.clarivate.wos.wos_api import WosAPI
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
        doctypes = self.data["static_data"]["summary"]["doctypes"]

        # Verificar que hay tipos de documento
        if doctypes["count"] == 0:
            self.datos_carga_publicacion.set_tipo("Otros")
            return

        tipo = doctypes["doctype"]

        if doctypes["count"] > 1:
            if "Article" in tipo:
                tipo = "Article"
            else:
                tipo = tipo[0]
        else:
            # Si solo hay un tipo, extraerlo de la lista
            tipo = tipo[0] if isinstance(tipo, list) else tipo

        tipos = {
            # TODO: Aclarar tipos pubs - a resolver
            "Article": "Artículo",
            "Meeting Abstract": "Meeting",
            "News Item": "Otros",
            "Proceedings Paper": "proceedings",
            "Review": "Revisión",
        }

        valor = tipos.get(tipo, "Otros")
        self.datos_carga_publicacion.set_tipo(valor)

    def _cargar_autores(
        self,
        tipo: str,
        attr_name: str,
    ):
        names_data = self.data["static_data"]["summary"]["names"]

        # Verificar que hay autores
        if names_data["count"] == 0:
            return

        autores = names_data["name"]
        if isinstance(autores, dict):
            autores = [autores]

        for autor in autores:
            # Se completa el Objeto DatosCargaAutor(Autor)
            firma = autor["display_name"]
            orden = autor["seq_no"]

            roles_autor = {
                "author": "Autor/a",
                "book_corp": "Grupo",
                "corp": "Grupo",
                "book_editor": "Editor/a",
            }

            tipo = roles_autor.get(autor["role"], "Autor/a")

            carga_autor = DatosCargaAutor(orden=orden, firma=firma, tipo=tipo)

            # Comprobar si es autor de correspondencia
            if autor.get("reprint") == "Y":
                carga_autor.contacto = "S"

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
            addresses_data = self.data["static_data"]["fullrecord_metadata"][
                "addresses"
            ]

            # Verificar que hay direcciones
            if addresses_data["count"] == 0:
                self.datos_carga_publicacion.add_autor(carga_autor)
                continue

            address_name = addresses_data.get("address_name")

            # Verificar que address_name no está vacío
            if not address_name or address_name == {}:
                self.datos_carga_publicacion.add_autor(carga_autor)
                continue

            addresses = (
                address_name if isinstance(address_name, list) else [address_name]
            )

            # Manejar addr_no como lista o entero
            autor_addr_no = autor.get("addr_no", [])
            if isinstance(autor_addr_no, int):
                autor_addr_no = [autor_addr_no]
            elif not autor_addr_no:
                self.datos_carga_publicacion.add_autor(carga_autor)
                continue

            for addr_no in autor_addr_no:
                address = next(
                    (
                        addr["address_spec"]
                        for addr in addresses
                        if addr.get("address_spec", {}).get("addr_no") == addr_no
                    ),
                    None,
                )

                if address and "organizations" in address:
                    organizations = address["organizations"].get("organization", [])
                    if isinstance(organizations, dict):
                        organizations = [organizations]

                    if organizations:
                        nombre = next(
                            (
                                org["content"]
                                for org in organizations
                                if org.get("pref") == "Y"
                            ),
                            organizations[0]["content"] if organizations else "",
                        )
                        afiliacion = DatosCargaAfiliacionesAutor(
                            nombre=nombre,
                            ciudad=address.get("city"),
                            pais=address.get("country"),
                        )
                        carga_autor.add_afiliacion(afiliacion)

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
        agno = datetime.strptime(fecha, "%Y-%m-%d").year
        agno_str = str(agno)
        mes = datetime.strptime(fecha, "%Y-%m-%d").month
        mes_str = f"{mes:02d}"
        if len(agno_str) != 4 or len(mes_str) != 2:
            raise TypeError("El mes o el año no tiene el formato correcto")
        fecha_insercion = DatosCargaFechaPublicacion(
            tipo="publicacion", agno=agno, mes=mes
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
                    valor = id["value"].removeprefix("MEDLINE:")
                    identificador = DatosCargaIdentificadorPublicacion(
                        valor=valor, tipo="pubmed"
                    )
                    self.datos_carga_publicacion.add_identificador(identificador)

    def cargar_volumen(self):
        valor = self.data["static_data"]["summary"]["pub_info"].get("vol")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="volumen", valor=str(valor))
        self.datos_carga_publicacion.add_dato(dato)

    # TODO: Revisar bien la diferencia entre número y número de artículo
    def cargar_numero(self):
        valor = self.data["static_data"]["summary"]["pub_info"].get("issue")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="numero", valor=str(valor))
        self.datos_carga_publicacion.add_dato(dato)

    def cargar_numero_art(self):
        identifier = self.data["dynamic_data"]["cluster_related"]["identifiers"].get(
            "identifier", []
        )
        if isinstance(identifier, dict):
            identifier = [identifier]

        for id in identifier:
            if id["type"] == "art_no":
                # Corregido: usar DatosCargaDatoPublicacion
                dato = DatosCargaDatoPublicacion(tipo="num_articulo", valor=id["value"])
                self.datos_carga_publicacion.add_dato(dato)

    # TODO: No P.Ini y P.fin - viene el numero de paginas totales
    def cargar_pag_inicio_fin(self):
        page_info = self.data["static_data"]["summary"]["pub_info"].get("page")
        if page_info:
            inicio = page_info.get("begin")
            fin = page_info.get("end")

            if inicio:
                dato_inicio = DatosCargaDatoPublicacion(
                    tipo="pag_inicio", valor=str(inicio)
                )
                self.datos_carga_publicacion.add_dato(dato_inicio)

            if fin:
                dato_fin = DatosCargaDatoPublicacion(tipo="pag_fin", valor=str(fin))
                self.datos_carga_publicacion.add_dato(dato_fin)

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
        publisher_data = self.data["static_data"]["summary"]["publishers"]["publisher"]
        names_data = publisher_data["names"]

        # Verificar que hay editores
        if names_data["count"] == 0:
            return

        if names_data["count"] == 1:
            name_info = names_data["name"]
            editorial = DatosCargaEditorial(name_info["display_name"])
            self.datos_carga_publicacion.fuente.add_editorial(editorial)
        else:
            names = names_data.get("name", [])
            if isinstance(names, dict):
                names = [names]
            for name_info in names:
                editorial = DatosCargaEditorial(name_info["display_name"])
                self.datos_carga_publicacion.fuente.add_editorial(editorial)

    def cargar_fuente(self):
        self.cargar_ids_fuente()
        self.cargar_titulo_y_tipo()
        self.carga_editorial()

    def cargar_financiacion(self):
        fund_ack = self.data["static_data"]["fullrecord_metadata"].get("fund_ack")
        if not fund_ack:
            return

        grants_data = fund_ack.get("grants", {})
        if grants_data.get("count", 0) == 0:
            return

        agencias = grants_data.get("grant", [])
        if isinstance(agencias, dict):
            agencias = [agencias]

        for agencia_obj in agencias:
            if "grant_ids" in agencia_obj and "grant_agency" in agencia_obj:
                agencia = agencia_obj.get("grant_agency")
                grant_ids_data = agencia_obj["grant_ids"]
                n_proyectos = grant_ids_data.get("count", 0)

                if n_proyectos > 1:
                    proyectos = grant_ids_data.get("grant_id", [])
                    if isinstance(proyectos, str):
                        proyectos = [proyectos]
                    for proyecto_obj in proyectos:
                        financiacion = DatosCargaFinanciacion(
                            agencia=agencia,
                            entidad_financiadora=agencia,
                            proyecto=proyecto_obj,
                        )
                        self.datos_carga_publicacion.add_financiacion(financiacion)
                elif n_proyectos == 1:
                    proyecto = grant_ids_data.get("grant_id")
                    financiacion = DatosCargaFinanciacion(
                        agencia=agencia,
                        entidad_financiadora=agencia,
                        proyecto=proyecto,
                    )
                    self.datos_carga_publicacion.add_financiacion(financiacion)

    def carga_acceso_abierto(self):
        oas = self.data["static_data"]["summary"]["pub_info"]["journal_oas_gold"]

        if oas == "Y":
            acceso_abierto = DatosCargaAccesoAbierto(valor="gold", origen="wos")
            self.datos_carga_publicacion.acceso_abierto.append(acceso_abierto)
