from integration.apis.api import API
import integration.apis.openalex.config as config
import re


class OpenalexAPI(API):
    # TODO: Rehacer con lista de trabajos
    def __init__(
        self,
        api_keys: list[str] = [],
        uri_template: str = "https://api.openalex.org/",
        uri_data: dict = {},
        route: str = "works",
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

    def search_get_from_id(self, id=id):
        self.set_param_mailto()
        self.set_param_id(id=id)
        self.get_respose(timeout=(5, 5))
        search_results: dict = self.response.get("results", {})  # TODO: cambiar message
        self.results = search_results
        return search_results

    def search_get_from_doi(self, id=id):
        self.set_param_mailto()
        self.set_param_doi(doi=id)
        self.get_respose(timeout=(5, 5))
        search_results: dict = self.response.get("results", {})  # TODO: cambiar message
        self.results = search_results
        return search_results

    def set_param_mailto(self):
        self.args["mailto"] = config.mail

    def set_param_doi(self, doi):
        self.args["filter"] = f"doi:{doi}"

    def set_param_id(self, id):
        self.args["filter"] = f"id.openalex:{id}"

    def set_headers_user_agent(self):
        self.headers["User-Agent"] = f"mailto:{config.mail}"
