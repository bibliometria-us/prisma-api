from integration.apis.api import API
import integration.apis.zenodo.config as config


class ZenodoAPI(API):
    def __init__(
        self,
        api_keys: list[str] = config.api_keys,
        uri_template: str = "https://zenodo.org/api/",
        uri_data: dict = {},
        route: str = "records",
        args: dict = {},
        headers: dict = {},
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

    def search(self):
        self.get_respose()
        pass

    def search_by_doi(self, doi: str):
        self.args["q"] = f'doi:"{doi}"'
        self.args["all_versions"] = 1
        self.search()
