from db.conexion import BaseDatos
from utils.date import get_current_date
from utils.format import table_to_pandas
import secrets


class AsyncRequest:
    def __init__(
        self,
        request_type: str = None,
        id: str = None,
        email: str = None,
        params: str = None,
    ) -> None:
        self.db = BaseDatos(database="api")
        self.request_type = request_type
        self.email = email
        self.status = "En proceso"
        self.result = ""
        self.params = params
        if not id:
            self.id = get_current_date(
                format=True, format_str="%Y%m%d%H%M%S%f"
            ) + secrets.token_hex(16)
            self.save()
        else:
            self.id = id
            self.load()

    def load(self):
        query = "SELECT id, tipo, parametros, estado, resultado, destinatario FROM peticion WHERE id = %(id)s"
        params = {"id": self.id}

        query_result = self.db.ejecutarConsulta(query, params)

        df = table_to_pandas(query_result).iloc[0]

        self.request_type = df["tipo"]
        self.params = df["parametros"]
        self.status = df["estado"]
        self.result = df["resultado"]
        self.email = df["destinatario"]

    def save(self):
        query = """REPLACE INTO peticion (id, tipo, parametros, estado, resultado, destinatario)
                VALUES (%(id)s,%(tipo)s,%(parametros)s,%(estado)s,%(resultado)s,%(destinatario)s)"""

        params = {
            "id": self.id,
            "tipo": self.request_type,
            "parametros": self.params,
            "estado": self.status,
            "resultado": self.result,
            "destinatario": self.email,
        }

        self.db.ejecutarConsulta(query, params)

    def close(self, message):
        self.status = "finalizado"
        self.result = message
        self.save()

    def error(self, message):
        self.status = "error"
        self.result = message
        self.save()
