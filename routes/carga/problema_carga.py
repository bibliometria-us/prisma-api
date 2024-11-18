from abc import ABC

from db.conexion import BaseDatos


class ProblemaCarga(ABC):
    def __init__(
        self,
        id_carga: str,
        db: BaseDatos,
        id_dato: str,
        antigua_fuente: str,
        antiguo_valor: str,
        nueva_fuente: str,
        nuevo_valor: str,
        tipo_dato_2: str = None,
        tipo_dato_3: str = None,
    ):
        self.id_carga = id_carga
        self.db = db
        self.tipo_problema = None
        self.revisado = False
        self.id_dato = id_dato
        self.tipo_dato_2 = tipo_dato_2
        self.tipo_dato_3 = tipo_dato_3
        self.antigua_fuente = antigua_fuente
        self.antiguo_valor = antiguo_valor
        self.nueva_fuente = nueva_fuente
        self.nuevo_valor = nuevo_valor
        self.mensaje = ""

    def advertencia(self, mensaje: str):
        self.tipo_problema = "Advertencia"
        self.mensaje = mensaje

    def error(self, mensaje: str):
        self.tipo_problema = "Error"
        self.mensaje = mensaje

    def es_valido(self) -> bool:
        return self.antiguo_valor != self.nuevo_valor

    def insertar(self):
        if not self.es_valido():
            return None

        query = """
                INSERT INTO prisma.a_problemas (
                    idCarga, tipo_problema, tipo_dato, id_dato, mensaje, 
                    tipo_dato_2, tipo_dato_3, antigua_fuente, antiguo_valor, nueva_fuente, nuevo_valor
                ) 
                VALUES (
                    %(idCarga)s, %(tipo_problema)s, %(tipo_dato)s, %(id_dato)s, %(mensaje)s, 
                    %(tipo_dato_2)s, %(tipo_dato_3)s, %(antigua_fuente)s, %(antiguo_valor)s, %(nueva_fuente)s, %(nuevo_valor)s
                );
                """

        params = {
            "idCarga": self.id_carga,
            "tipo_problema": self.tipo_problema,
            "tipo_dato": self.tipo_dato,
            "id_dato": self.id_dato,
            "mensaje": self.mensaje,
            "tipo_dato_2": self.tipo_dato_2,
            "tipo_dato_3": self.tipo_dato_3,
            "antigua_fuente": self.antigua_fuente,
            "antiguo_valor": self.antiguo_valor,
            "nueva_fuente": self.nueva_fuente,
            "nuevo_valor": self.nuevo_valor,
        }

        self.db.ejecutarConsulta(query, params)
