from integration.apis.api import API
import integration.apis.openalex.config as config
import re


class OpenalexAPI(API):
    def __init__(
        self,
        api_keys: list[str] = [],
        uri_template: str = "https://api.openalex.org/",
        uri_data: dict = {},
        route: str = "works/",
        args: dict = {},
        headers: dict = {
            "Accept": "application/json",
            "User-Agent": "MyCustomClient/1.0",
        },
        json: dict = {},
        response_type: str = "json",
    ):
        super().__init__(
            api_keys, uri_template, uri_data, route, args, headers, json, response_type
        )
        self.results = []

    # def set_api_key(self):
    #     super().set_api_key()
    #     self.args["access_token"] = self.api_key

    # def set_headers_complete_view(self):
    #     self.headers["Accept"] = "application/vnd.inveniordm.v1+json"

    def search(self, doi=""):
        self.get_respose(id=doi, timeout=(5, 5))
        search_results: dict = self.response.get("results", {})  # TODO: cambiar message
        self.results = search_results

    def set_param_mailto(self):
        self.args["mailto"] = config.mail

    def set_headers_user_agent(self):
        self.headers["User-Agent"] = f"mailto:{config.mail}"

    def search_by_doi(self, doi: str):
        self.set_param_mailto()
        # self.set_headers_user_agent()
        self.search(doi=doi)
        # TODO: controlar si no devuelve resultados o si devuelve mas de uno
        assert self.results["total"] != "1"
        return self.results["results"]
