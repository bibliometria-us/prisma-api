from integration.apis.api import API
import integration.apis.zenodo.config as config
import re


class ZenodoAPI(API):
    def __init__(
        self,
        api_keys: list[str] = config.api_keys,
        uri_template: str = "https://zenodo.org/api/",
        uri_data: dict = {},
        route: str = "records",
        args: dict = {},
        headers: dict = {"Accept": "application/vnd.inveniordm.v1+json"},
        json: dict = {},
        response_type: str = "json",
    ):
        super().__init__(
            api_keys, uri_template, uri_data, route, args, headers, json, response_type
        )
        self.results = []

    def set_api_key(self):
        super().set_api_key()
        self.args["access_token"] = self.api_key

    def set_headers_complete_view(self):
        self.headers["Accept"] = "application/vnd.inveniordm.v1+json"

    def search(self):
        self.get_respose()
        search_results: dict = self.response.get("hits", {})  # TODO: cambiar message
        self.results = search_results

    def search_by_doi(self, doi: str):
        self.set_api_key()
        self.set_headers_complete_view()
        # escaped_doi = re.escape(doi)
        self.args["q"] = f"doi:{doi}"
        self.args["all_versions"] = 1
        self.search()
        # TODO: controlar si no devuelve resultados o si devuelve mas de uno
        assert self.results["total"] != "1"
        return self.results["hits"]
