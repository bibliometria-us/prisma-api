from time import sleep
import requests
import abc

from integration.apis.exceptions import APIRateLimit


class API:
    def __init__(
        self,
        api_keys: list[str] = None,
        uri_template: str = None,
        uri_data: dict = {},
        route: str = "",
        args: dict = {},
        headers: dict = {},
        json: dict = {},
        response_type: str = None,
    ):
        self.api_keys = api_keys
        self.uri_template = uri_template
        self.uri_data = uri_data
        self.route = route
        self.args = args
        self.headers = headers
        self.json = json
        self.uri = None
        self.response_type = response_type
        self.format_uri()
        self.response: dict = None
        self.api_key_index = 0
        self.retries = 3
        self.set_api_key()
        self.proxies = {
            "http": "http://proxy.int.local:3128",
            "https": "http://proxy.int.local:3128",
        }
        # TODO: proxies en config global

    @abc.abstractmethod
    def set_api_key(self):
        try:
            if self.api_keys:
                self.api_key = self.api_keys[self.api_key_index]

        except Exception:
            raise APIRateLimit()

    def roll_api_key(self):
        self.api_key_index += 1
        if self.api_key_index >= len(self.api_keys) - 1:
            if self.retries > 0:
                retries = retries - 1
                self.api_key_index = 0
        self.set_api_key()

    def format_uri(self) -> None:
        if self.uri_template:
            self.uri = self.uri_template.format(**self.uri_data)
        if self.route:
            self.uri = self.uri + self.route

    def get_respose(
        self,
        request_method="GET",
        id="",
        timeout=None,
        proxies=True,
        tryouts=5,
        **kwargs,
    ) -> dict:
        json = None
        response_type_to_function = {"json": self.get_json_response}

        function = response_type_to_function.get(self.response_type)

        if proxies:
            proxy_list = self.proxies
        else:
            proxy_list = {}

        request_method = request_method.lower()
        if not request_method == "get":
            json = self.json

        try:
            response = requests.request(
                method=request_method.lower(),
                url=self.uri + id,
                headers=self.headers,
                params=self.args,
                json=json,
                timeout=timeout or (5, 5),
                proxies=proxy_list,
                **kwargs,
            )
        except Exception as e:
            if tryouts > 0:
                tryouts -= 1
                self.get_respose(
                    request_method, id, timeout, proxies, tryouts, **kwargs
                )
        if response.status_code == 200:
            pass
        if response.status_code in [401, 429] and self.api_keys:
            self.roll_api_key()
            sleep(1)
            self.get_respose(
                request_method=request_method, id=id, timeout=timeout, kwargs=kwargs
            )
            return None
        if response.status_code != 200:
            return None

        result = function(response)

        return result

    def get_json_response(self, response: requests.Response) -> dict:

        result = response.json()
        self.response = result
        return result

    def set_uri_template(self, uri_template) -> None:
        self.uri_template = uri_template

    def add_uri_data(self, items) -> None:
        self.uri_data.update(items)

    def add_headers(self, items: dict) -> None:
        self.headers.update(items)

    def add_args(self, args: dict) -> None:
        self.args.update(args)

    def add_json_data(self, json: dict) -> None:
        self.json.update(json)
