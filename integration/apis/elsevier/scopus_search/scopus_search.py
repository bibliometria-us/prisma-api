from typing import List
from integration.apis.api import API
from integration.apis.elsevier import config


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
        # DUDA: que proposito tiene esto ?
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

    def get_from_id(self, id: str):
        # TODO: Ver si realmente interesa poener esto aquí o a nivel de constructor
        self.set_headers_key()
        self.set_complete_view(True)
        # Controla que incluya el prefijo
        prefijo = "2-s2.0-"
        if id.startswith(prefijo):
            query = f"EID({id})"  # El texto ya tiene el prefijo, lo devolvemos tal cual
        else:
            query = f"{prefijo}{id}"

        self.args["query"] = query

        self.search()
        # TODO: controlar que el resultado no venga vacío
        return self.results
