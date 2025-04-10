import re
from typing import List
from integration.apis.api import API
from integration.apis.elsevier import config
from urllib.parse import quote


class ScopusSearch(API):
    """
    Clase que representa el objeto de conexión con la API de Scopus.
    """

    def __init__(
        self,
        api_keys: list[str] = [config.api_key],
        uri_template: str = "https://api.elsevier.com/content/search/scopus",
        uri_data: dict = {},
        route: str = "",
        args: dict = {},
        headers: dict = {},
        json: dict = {},
        response_type: str = "json",
        complete_view: bool = True,
    ):
        self.inst_token = config.inst_token_key
        super().__init__(
            api_keys, uri_template, uri_data, route, args, headers, json, response_type
        )
        self.max_length = 25
        self.page = 0
        self.results = []
        self.args["view"] = "STANDARD" if not complete_view else "COMPLETE"

    def set_api_key(self):
        super().set_api_key()

    def set_headers_key(self):
        self.headers["X-ELS-APIKey"] = self.api_key
        self.headers["X-ELS-Insttoken"] = self.inst_token

    def set_complete_view(self, complete_view: bool):
        self.complete_view = complete_view

    def search(self):
        self.get_respose()

        search_results: dict = self.response.get("search-results", {})
        self.results = search_results.get("entry", [])

    def search_pag(self):
        """
        Método que ejecuta la paginación completa de la búsqueda y almacena los resultados.
        """
        self.results = (
            []
        )  # Asegurar que la lista de resultados esté vacía antes de comenzar la búsqueda
        next_cursor = "*"

        while next_cursor:
            self.args["cursor"] = f"{next_cursor}"
            self.get_respose()

            if not self.response:
                return []

            search_results: dict = self.response.get("search-results", {})
            total_results = int(search_results.get("opensearch:totalResults", 0))

            if total_results == 0:
                return []

            self.results.extend(search_results.get("entry", []))

            # Determinar si hay una siguiente página
            cursor_data = search_results.get("cursor", {})
            next_cursor = (
                cursor_data.get("@next") if total_results > len(self.results) else None
            )

        return self.results

    def get_publicaciones_por_id(self, id_pub: str):
        """
        Método para obtener las publicaciones por id de Scopus.
        """
        scopus_regex = r"^2-s2\.0-\d{11}$"
        if not re.match(scopus_regex, id_pub, re.IGNORECASE):
            raise ValueError(
                f"'{id_pub}' no tiene un formato válido de identificador Scopus."
            )

        self.set_headers_key()
        self.set_complete_view(True)

        query = f"EID({id_pub})"
        self.args["query"] = query

        return self.search_pag()

    def get_publicaciones_por_doi(self, id_pub: str):
        """
        Método para obtener las publicaciones por DOI.
        """
        doi_regex = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"
        if not re.match(doi_regex, id_pub, re.IGNORECASE):
            raise ValueError(f"'{id_pub}' no tiene un formato válido de DOI.")

        self.set_headers_key()
        self.set_complete_view(True)

        query = f"DOI({id_pub})"
        self.args["query"] = query

        return self.search_pag()

    def get_publicaciones_por_investigador_fecha(
        self, id_inves: str, agno_inicio: str = None, agno_fin: str = None
    ):
        """
        Método para obtener las publicaciones por id de investigador dentro de un período de años.
        """
        scopus_regex = r"^\d{10,11}$"
        if not re.match(scopus_regex, id_inves, re.IGNORECASE):
            raise ValueError(
                f"'{id_inves}' no tiene un formato válido de identificador Scopus."
            )

        self.set_headers_key()
        self.set_complete_view(True)

        query = f"AU-ID({id_inves})"
        if agno_inicio and agno_fin:
            agno_fin = int(agno_fin) + 1
            agno_inicio = int(agno_inicio) - 1
            query += f" AND PUBYEAR < {agno_fin} AND PUBYEAR > {agno_inicio}"

        self.args["query"] = query

        # Ejecutar la búsqueda paginada y devolver resultados
        return self.search_pag()
