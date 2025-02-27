from typing import List
from integration.apis.api import API
from integration.apis.crossref import config


class CrossrefAPI(API):
    """
    Clase que representa el objeto de conexión con la API de Crossref.
    """

    def __init__(
        self,
        api_keys: list[str] = [],
        uri_template: str = "https://api.crossref.org/",
        uri_data: dict = {},
        route: str = "works/",
        args: dict = {},
        headers: dict = {},
        json: dict = {},
        response_type: str = "json",
        complete_view: bool = True,
    ):
        super().__init__(
            api_keys, uri_template, uri_data, route, args, headers, json, response_type
        )
        # self.max_length = 25
        # self.page = 0
        self.results = []

    def set_api_key(self):
        # super().set_api_key()
        pass

    def set_headers_rate_limits(self):
        self.headers["X-Rate-Limit-Limit"] = "50"
        self.headers["X-Rate-Limit-Interval"] = "15"

    def set_headers_key(self):
        self.headers["X-ELS-APIKey"] = config.api_key

    def set_param_mailto(self):
        self.args["mailto"] = config.mail

    def get_publicaciones_por_doi(self, id: str):
        # TODO: Ver si realmente interesa poener esto aquí o a nivel de constructor
        self.set_headers_key()
        self.set_headers_rate_limits()
        self.set_param_mailto()
        self.get_respose(id=id)  # TODO: definir url correctamente

        search_results: dict = self.response.get("message", {})
        self.results = search_results

        # TODO: controlar que el resultado no venga vacío
        return self.results
