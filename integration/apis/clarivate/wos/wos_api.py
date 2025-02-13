from typing import List
from integration.apis.api import API
from integration.apis.clarivate.wos import config


class WosAPI(API):
    def __init__(
        self,
        api_keys: list[str] = config.api_key,
        uri_template: str = "https://wos-api.clarivate.com/api/wos",
        uri_data: dict = {},
        json: dict = {
            "databaseId": "WOS",
            "count": 100,
            "firstRecord": 1,
        },
        headers: dict = {
            "accept": "application/json",
            "Content-Type": "application/json",
        },
        response_type: str = "json",
        records: list = [],
    ):

        super().__init__(
            uri_template=uri_template,
            uri_data=uri_data,
            headers=headers,
            json=json,
            response_type=response_type,
            api_keys=api_keys,
        )
        self.records = records
        self.records_found = 0

    def set_api_key(self):
        super().set_api_key()

        self.headers["X-ApiKey"] = self.api_key

    def search_by_DOI_list(self, doi_list: List[str]):
        query = f"DO=({' OR '.join([doi for doi in doi_list])})"

        self.route = "/"
        self.format_uri()
        self.add_json_data({"usrQuery": query})

        self.search()

    def search(self, page=0):
        self.get_respose(request_method="POST")

        self.json["firstRecord"] = self.json["count"] * page + 1

        self.records_found = self.response["QueryResult"]["RecordsFound"]
        if self.records_found == 0:
            return None

        records = self.response["Data"]["Records"]["records"]["REC"]

        self.records += records

        if self.records_found > self.json["firstRecord"] + self.json["count"]:
            self.search(page=page + 1)

    def get_from_id(self, id: str):
        self.route = "/"
        # TODO: Ver si realmente interesa poener esto aquí o a nivel de constructor
        self.set_api_key()
        # TODO: dataBaseId WOS o WOK ?
        dataBaseId = f"WOS"

        # Controla que incluya el prefijo
        # TODO: distinguir WOS y Medline
        prefijo = "WOS:"
        query = f"UT=({id if id.startswith(prefijo) else prefijo + id})"

        body_args = {}

        body_args["usrQuery"] = query
        body_args["databaseId"] = dataBaseId

        self.add_json_data(body_args)

        self.search()
        # TODO: controlar que el resultado no venga vacío
        # Se devuelve un resultado ya que es una petición de una pub por id
        return self.records[0]

    def get_from_doi(self, id: str):
        self.route = "/"
        # TODO: Ver si realmente interesa poener esto aquí o a nivel de constructor
        self.set_api_key()
        # TODO: dataBaseId WOS o WOK ?
        dataBaseId = f"WOS"

        # Controla que incluya el prefijo
        # TODO: distinguir WOS y Medline
        prefijo = "WOS:"
        query = f"DO=({id if id.startswith(prefijo) else prefijo + id})"

        body_args = {}

        body_args["usrQuery"] = query
        body_args["databaseId"] = dataBaseId

        self.add_json_data(body_args)

        self.search()
        # TODO: controlar que el resultado no venga vacío
        # Se devuelve un resultado ya que es una petición de una pub por id
        return self.records[0]

    def get_metrics_from_id(self, id: str):
        self.route = "/citing"
        self.set_api_key()
        dataBaseId = f"WOS"

        # Controla que incluya el prefijo
        # TODO: distinguir WOS y Medline
        prefijo = "WOS:"
        uniqueId = f"{id if id.startswith(prefijo) else prefijo + id}"

        body_args = {}

        body_args["UniqueId"] = uniqueId
        body_args["databaseId"] = dataBaseId

        self.add_json_data(body_args)

        self.search()
        # TODO: controlar que el resultado no venga vacío
        # Se devuelve un resultado ya que es una petición de una pub por id
        return self.records[0]
