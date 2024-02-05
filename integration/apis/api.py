import requests

from integration.apis.exceptions import APIRateLimit

class API:
    def __init__(self, uri_template:str = None, uri_data:dict = {}, headers:dict = {}, response_type:str=None):
        self.uri_template = uri_template
        self.uri_data = uri_data
        self.headers = headers
        self.uri = None
        self.response_type = response_type
        self.format_uri()
        self.response = None

    def format_uri(self) -> None:
        if self.uri_template:
            self.uri = self.uri_template.format(**self.uri_data)
    
    def get_respose(self) -> dict:
        response_type_to_function = {
            "json": self.get_json_response
        }
        
        function = response_type_to_function.get(self.response_type)

        response = requests.get(self.uri, headers=self.headers)

        if response.status_code == 200:
            pass
        if response.status_code == 429:
            raise APIRateLimit()

        result = function(response)
        
        return result

    def get_json_response(self, response) -> dict:

        result = response.json()
        self.response = result
        return result
        
    def set_uri_template(self, uri_template) -> None:
        self.uri_template = uri_template

    def add_uri_data(self, items) -> None:
        self.uri_data.update(items)

    def add_headers(self, items) -> None:
        self.headers.update(items)