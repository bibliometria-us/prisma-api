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

    def search_pag(self):
        """
        Método que ejecuta la paginación completa de la búsqueda y almacena los resultados.
        """
        self.results = (
            []
        )  # Asegurar que la lista de resultados esté vacía antes de comenzar la búsqueda
        next_cursor = "*"

        while next_cursor:
            self.args["cursor"] = f"{next_cursor}"
            self.get_respose()
            meta: dict = self.response.get("meta", {})
            search_results: dict = self.response.get("results", {})
            total_results = int(meta.get("count", 0))

            if total_results == 0:
                raise ValueError("No se encontraron resultados.")

            self.results.extend(search_results)

            # Determinar si hay una siguiente página
            next_cursor = (
                meta.get("next_cursor", None)
                if total_results > len(self.results)
                else None
            )

        return self.results

    def set_param_mailto(self):
        self.args["mailto"] = config.mail

    def set_param_filter(self, filter):
        self.args["filter"] = filter

    def set_headers_user_agent(self):
        self.headers["User-Agent"] = f"mailto:{config.mail}"

    def get_publicaciones_por_id(self, id=id):
        self.set_param_mailto()
        self.set_param_id(id=id)
        self.set_param_filter(filter=f"id.openalex:{id}")

        return self.search_pag()

    def get_publicaciones_por_doi(self, id=id):
        self.set_param_mailto()
        self.set_param_filter(filter=f"doi:{id}")

        return self.search_pag()

    def get_publicaciones_por_investigador_fecha(
        self, id_inves: str, agno_inicio: str = None, agno_fin: str = None
    ):

        openalex_regex = r"A(\d+)"
        if not re.match(openalex_regex, id_inves, re.IGNORECASE):
            raise ValueError(
                f"'{id_inves}' no tiene un formato válido de identificador OpenAlex."
            )

        self.set_param_mailto()
        filter = f'authorships.author.id:"{id_inves}")'
        if agno_inicio and agno_fin:
            agno_fin = int(agno_fin) + 1
            agno_inicio = int(agno_inicio) - 1
            filter += f",publication_year:{agno_inicio}-{agno_fin}"
        self.set_param_filter(filter=filter)

        return self.search_pag()
