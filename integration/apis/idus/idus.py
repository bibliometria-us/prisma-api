import requests
from integration.apis.api import API
from integration.apis.idus import config


class IdusAPI(API):
    def __init__(
        self,
        route: str = None,
    ):
        super().__init__(
            uri_template=config.url_api_idus,
            response_type="json",
            route=route,
        )

    def get_respose(self, request_method="GET", id="", timeout=3, tryouts=5) -> dict:
        proxies = {
            "http": "",
            "https": "",
        }
        if tryouts == 0:
            return None
        try:
            return super().get_respose(request_method, id, timeout, proxies=proxies)
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ProxyError,
            requests.exceptions.ConnectTimeout,
        ) as e:
            tryouts -= 1
            timeout += 2
            self.get_respose(tryouts=tryouts, timeout=timeout)
        except Exception as e:
            return None


class IdusAPISearch(IdusAPI):
    def __init__(
        self,
        route: str = "/discover/search/objects",
    ):
        super().__init__(
            route=route,
        )

    def get_from_handle(self, handle: str) -> dict:
        args = {
            "query": f"handle:{handle}",
        }
        self.add_args(args)
        self.get_respose()

        return self.response

    def get_uuid(self, index: int = 0):
        return self.response["_embedded"]["searchResult"]["_embedded"]["objects"][
            index
        ]["_embedded"]["indexableObject"]["uuid"]


class IdusAPIItems(IdusAPI):
    def __init__(
        self,
        route: str = "/core/items/",
    ):
        super().__init__(
            route=route,
        )

    def get_from_uuid(self, uuid: str) -> dict:
        self.get_respose(id=uuid)

        return self.response

    def get_from_handle(self, handle: str) -> dict:
        search = IdusAPISearch()
        search.get_from_handle(handle)

        uuid = search.get_uuid()
        self.get_from_uuid(uuid)

        return self.response
