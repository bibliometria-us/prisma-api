from integration.apis.clarivate.wos.wos_api import WosAPI
from integration.apis.crossref.crossref.crossref import CrossrefAPI
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


class CrossrefParser(Parser):
    def __init__(self, idCrossref: str) -> None:
        # Se inicializa la clase padre
        # La clase padre Parser tiene el atributo datos_carga_publicacion
        super().__init__()
        # Se definen los atributos de la clase
        self.idCrossref = idCrossref
        self.data: dict = None
        self.api_request()  # Se hace la petición de Xref
        self.carga()  # Con los datos recuperados, se rellena el objeto datos_carga_publicacion

    def set_fuente_datos(self):
        self.datos_carga_publicacion.set_fuente_datos("Crossref")

    def api_request(self):
        api = CrossrefAPI()
        response = api.get_from_doi(self.idCrossref)
        self.data = response

    def cargar_titulo(self):
        self.datos_carga_publicacion.set_titulo(self.data["title"])

    def cargar_titulo_alternativo(self):
        # TODO: Titulo Alt - revisar en titles
        pass

    def cargar_tipo(self):
        tipo = self.data["type"]

        tipos = {
            # TODO: Aclarar tipos pubs - a resolver
            "journal-article": "Artículo",
            "proceedings-article": "Ponencia",
            "book-chapter": "Capítulo",
            "book": "Libro",
            "other": "Otros",
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
        cont = 1  # TODO: orden - sol temporal
        for autor in self.data.get(attr_name, []):
            # Se completa el Objeto DatosCargaAutor(Autor)
            firma = f"{autor['family']}, {autor['given']}"
            orden = cont
            cont += 1
            # TODO: Aclarar rol autor - en este caso author, comprobar que nombre es en Prisma
            carga_autor = DatosCargaAutor(orden=orden, firma=firma, tipo=tipo)

            # Se completa el Objeto DatosCargaIdentificadorAutor(Identificador del Autor ORCID)
            if "ORCID" in autor:
                id_autor_orcid = DatosCargaIdentificadorAutor(
                    tipo="orcid", valor=autor.get("ORCID")
                )
                carga_autor.add_id(id_autor_orcid)  # Lo añadimos al objeto de Autor

            self.datos_carga_publicacion.add_autor(carga_autor)

    def cargar_autores(self):
        self._cargar_autores(
            tipo="Autor/a",
            attr_name="author",
        )

    def cargar_editores(self):
        self._cargar_autores(
            tipo="Editor/a",
            attr_name="editor",
        )

    # TODO: Introducir editores - Revisar donde está en el JSON

    def cargar_directores(self):
        pass

    def cargar_año_publicacion(self):
        año = self.data["published"]["date-parts"][0][0]
        # TODO: Control Excep - esto se debería recoger en un nivel superior (de hecho se comprueba 2 veces arriba)
        assert len(str(año)) == 4

        self.datos_carga_publicacion.set_año_publicacion(año)

    def cargar_mes_publicacion(self):
        # Si viene mas informacion que el año
        if len(self.data["published"]["date-parts"][0]) < 1:
            mes = self.data["published"]["date-parts"][0][1]
            # TODO: Control Excep - esto se debería recoger en un nivel superior (de hecho se comprueba 2 veces arriba)
            mes_formateado = f"{mes:02d}"
            assert len(str(mes_formateado)) == 2

            self.datos_carga_publicacion.set_mes_publicacion(mes)

    def cargar_fecha_publicacion(self):
        if len(self.data["published"]["date-parts"][0]) == 3:
            agno = self.data["published"]["date-parts"][0][0]
            mes = self.data["published"]["date-parts"][0][1]
            dia = self.data["published"]["date-parts"][0][2]
            fecha = f"{agno}-{mes}-{dia}"
            self.datos_carga_publicacion.set_fecha_publicacion(fecha)

    def cargar_identificadores(self):
        # Identificador ppal WOS
        identificador_doi = DatosCargaIdentificadorPublicacion(
            valor=self.data["DOI"], tipo="doi"
        )
        self.datos_carga_publicacion.add_identificador(identificador_doi)

    def cargar_volumen(self):
        valor = self.data.get("volume")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="volumen", valor=valor)
        self.datos_carga_publicacion.add_dato(dato)

    def cargar_numero(self):
        valor = self.data.get("issue")
        if not valor:
            return None
        dato = DatosCargaDatoPublicacion(tipo="numero", valor=valor)
        self.datos_carga_publicacion.add_dato(dato)

    # def cargar_numero_art(self):
    #     for id in self.data["dynamic_data"]["cluster_related"]["indentifiers"][
    #         "indentifier"
    #     ]:
    #         match id["type"]:
    #             # Esto es una excepcion, pero se guarda en IDs en el JSON
    #             case "art_no":
    #                 identificador = DatosCargaIdentificadorPublicacion(
    #                     valor=id["value"], tipo="num_articulo"
    #                 )
    #                 self.datos_carga_publicacion.add_identificador(identificador)

    # TODO: No P.Ini y P.fin - viene el numero de paginas totales
    def cargar_pag_inicio_fin(self):
        if (
            "page" in self.data and self.data["page"]
        ):  # Verifica que existe y no es None o vacío
            paginas = self.data["page"].split("-")

            if len(paginas) >= 1 and paginas[0].strip():  # Caso: página inicial existe
                dato_inicio = DatosCargaDatoPublicacion(
                    tipo="pag_inicio", valor=paginas[0].strip()
                )
                self.datos_carga_publicacion.add_dato(dato_inicio)

            if len(paginas) > 1 and paginas[1].strip():  # Caso: página final existe
                dato_fin = DatosCargaDatoPublicacion(
                    tipo="pag_fin", valor=paginas[1].strip()
                )
                self.datos_carga_publicacion.add_dato(dato_fin)

    def cargar_datos(self):
        if self.datos_carga_publicacion.es_tesis():
            return None
        self.cargar_volumen()
        self.cargar_numero()
        # self.cargar_numero_art()
        self.cargar_pag_inicio_fin()

    def cargar_ids_fuente(self):
        for id_fuente in self.data.get("issn-type", []):
            valor = id_fuente["value"]
            tipo = ""
            match id_fuente["type"]:
                case "print":
                    tipo = "issn"
                case "electronic":
                    tipo = "eissn"

            if tipo != "":
                self.datos_carga_publicacion.fuente.add_identificador(
                    DatosCargaIdentificadorFuente(valor=valor, tipo=tipo)
                )
        for id_fuente in self.data.get("isbn-type", []):
            valor = id_fuente["value"]
            tipo = ""
            match id_fuente["type"]:
                case "print":
                    tipo = "isbn"
                case "electronic":
                    tipo = "eisbn"
            if tipo != "":
                self.datos_carga_publicacion.fuente.add_identificador(
                    DatosCargaIdentificadorFuente(valor=valor, tipo=tipo)
                )

    def cargar_titulo_y_tipo(self):
        # TODO: Aclarar tipos de fuentes
        tipos_fuente = {"Revista": "Revista", "Libro": "Libro", "": "Revista"}
        tipo_fuente = ""
        # Titulo
        titulo = self.data["container-title"][0]
        # Necesario crear los identificadores previamente
        identificadores = self.datos_carga_publicacion.fuente.identificadores
        if any(obj.tipo == "isbn" or obj.tipo == "eisbn" for obj in identificadores):
            tipo_fuente = tipos_fuente["Libro"]
        if any(obj.tipo == "issn" or obj.tipo == "eissn" for obj in identificadores):
            tipo_fuente = tipos_fuente["Revista"]

        self.datos_carga_publicacion.fuente.set_titulo(titulo)
        self.datos_carga_publicacion.fuente.set_tipo(tipo_fuente)

    def carga_editorial(self):
        # TODO: 2 Editoriales - puede darse el caso ?
        editorial = DatosCargaEditorial(nombre=self.data.get("publisher", ""))
        if editorial.nombre != "":
            self.datos_carga_publicacion.fuente.editoriales.append(editorial)
            # TODO: Sin Editorial - Controlar bien que hacer cuando no viene editorial

    def cargar_fuente(self):
        self.cargar_ids_fuente()
        self.cargar_titulo_y_tipo()
        self.carga_editorial()
