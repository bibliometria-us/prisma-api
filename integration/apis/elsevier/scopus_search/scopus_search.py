from typing import List
from integration.apis.api import API
from integration.apis.elsevier import config


class ScopusSearch(API):
    def __init__(
        self,
        api_keys: list[str] = config.api_key,
        uri_template: str = "https://api.elsevier.com/content/search/scopus",
        uri_data: dict = {},
        route: str = "",
        args: dict = {},
        headers: dict = {},
        json: dict = {},
        response_type: str = "json",
        complete_view: bool = False,
    ):
        super().__init__(
            api_keys, uri_template, uri_data, route, args, headers, json, response_type
        )
        self.inst_token = config.inst_token_key
        self.max_length = 25
        self.page = 0
        self.results = []
        self.args["view"] = "STANDARD" if not complete_view else "COMPLETE"

    def set_api_key(self):
        super().set_api_key()
        self.headers["X-ELS-APIKey"] = self.api_key
        self.headers["X-ELS-Insttoken"] = self.inst_token

    def search(self):
        self.get_respose()

        search_results: dict = self.response["search-results"]
        entry = search_results.get("entry", [])
        self.results = entry

    def search_by_DOI_list(self, doi_list: List[str]):
        if len(doi_list) > self.max_length:
            doi_sublists = [
                doi_list[x : x + self.max_length]
                for x in range(0, len(doi_list), self.max_length)
            ]

            result = []

            for sublist in doi_sublists:
                sublist_result = self.search_by_DOI_list(sublist)
                result += sublist_result

            self.results = result
            return self.results

        query = " OR ".join(f"DOI({doi})" for doi in doi_list)
        self.args["query"] = query
        self.args["start"] = self.max_length * self.page

        self.search()

        return self.results
