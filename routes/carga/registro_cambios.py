from datetime import datetime

from db.conexion import BaseDatos
import secrets


class RegistroCambios:
    def __init__(
        self,
        tabla: str,
        id: int,
        tipo_dato: str,
        tipo_dato_2: str,
        tipo_dato_3: str,
        valor: str,
        autor: str = "Unidad de Bibliometría",
        origen: str = "Origen desconocido",
        bd: BaseDatos = None,
    ):
        self.tabla = tabla
        self.tabla_problemas = tabla.replace("cambios", "problemas")
        self.id = id
        self.tipo_dato = tipo_dato
        self.tipo_dato_2 = tipo_dato_2
        self.tipo_dato_3 = tipo_dato_3
        self.valor = valor
        self.origen = origen
        self.autor = autor
        self.fecha = datetime.now()
        self.comentario = None
        self.ultimo_registro: RegistroCambios = None
        self.bd = bd or BaseDatos(database=None)
        self.id_carga = None
        self.generar_comentario()

    @staticmethod
    def generar_id_carga():
        return datetime.now().strftime("%Y%m%d%H%M%S%f")[:17] + "".join(
            secrets.choice(
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            )
            for _ in range(23)
        )

    def insertar(self, id_carga=None):
        query = f"""
            INSERT INTO prisma.{self.tabla} (id, tipo_dato, tipo_dato_2, tipo_dato_3, valor, origen, fecha, comentario, id_carga)
                VALUES (%(id)s, %(tipo_dato)s, %(tipo_dato_2)s, %(tipo_dato_3)s, %(valor)s, %(origen)s, %(fecha)s, %(comentario)s, %(id_carga)s)
                """
        self.id_carga = id_carga
        if not self.id_carga:
            self.id_carga = self.generar_id_carga()

        params = {
            "id": self.id,
            "tipo_dato": self.tipo_dato,
            "tipo_dato_2": self.tipo_dato_2,
            "tipo_dato_3": self.tipo_dato_3,
            "valor": self.valor,
            "origen": self.origen,
            "fecha": self.fecha,
            "comentario": self.comentario,
            "id_carga": self.id_carga,
        }

        self.bd.ejecutarConsulta(query, params)

    def generar_comentario(self):
        pass

    def buscar_ultimo_registro(self, valor_actual=None):
        query = f"""
            SELECT * FROM prisma.{self.tabla}
            WHERE id = %(id)s
            AND tipo_dato = %(tipo_dato)s
            AND (tipo_dato_2 = %(tipo_dato_2)s OR %(tipo_dato_2)s IS NULL)
            AND (tipo_dato_3 = %(tipo_dato_3)s OR %(tipo_dato_3)s IS NULL)
            ORDER BY fecha DESC
            LIMIT 1
        """
        params = {
            "id": self.id,
            "tipo_dato": self.tipo_dato,
            "tipo_dato_2": self.tipo_dato_2,
            "tipo_dato_3": self.tipo_dato_3,
        }

        self.bd.ejecutarConsulta(query, params)
        df = self.bd.get_dataframe()
        data = None

        if not df.empty:
            data = df.iloc[0].to_dict()

        if not data and not valor_actual:
            return None

        if data:
            if valor_actual and (valor_actual != data["valor"]):
                data["valor"] = valor_actual
                data["origen"] = "Origen desconocido"

            registro = RegistroCambios(
                tabla=self.tabla,
                id=data["id"],
                tipo_dato=data["tipo_dato"],
                tipo_dato_2=data["tipo_dato_2"],
                tipo_dato_3=data["tipo_dato_3"],
                valor=data["valor"],
                origen=data["origen"],
                bd=self.bd,
            )

        if not data:
            registro = RegistroCambios(
                tabla=self.tabla,
                id=self.id,
                tipo_dato=self.tipo_dato,
                tipo_dato_2=self.tipo_dato_2,
                tipo_dato_3=self.tipo_dato_3,
                valor=valor_actual,
                origen="Origen desconocido",
                bd=self.bd,
            )

        registro.__class__ = self.__class__
        registro.generar_comentario()
        self.ultimo_registro = registro

        return registro

    def detectar_conflicto(self, valor_actual=None) -> "ProblemaCarga":
        self.buscar_ultimo_registro(valor_actual=valor_actual)
        if not self.ultimo_registro:
            return None
        if self.valor != self.ultimo_registro.valor:
            problema = self.crear_problema()
            return problema
        return None

    def crear_problema(self):
        ultimo_registro = self.ultimo_registro

        problema = ProblemaCarga(
            tabla=self.tabla_problemas,
            id=self.id,
            tipo_dato=self.tipo_dato,
            tipo_dato_2=self.tipo_dato_2,
            tipo_dato_3=self.tipo_dato_3,
            valor=self.valor,
            origen=self.origen,
            valor_antiguo=ultimo_registro.valor,
            origen_antiguo=ultimo_registro.origen,
            fecha=self.fecha,
            comentario=f"ANTIGUO: {ultimo_registro.comentario} NUEVO: {self.comentario}",
            bd=self.bd,
            id_carga=self.id_carga,
        )

        return problema


class ProblemaCarga:
    def __init__(
        self,
        tabla: int,
        id: int,
        tipo_dato: str,
        tipo_dato_2: str,
        tipo_dato_3: str,
        valor: str,
        origen: str,
        valor_antiguo: str,
        origen_antiguo: str,
        fecha: datetime,
        comentario: str,
        bd: BaseDatos,
        id_carga: str,
    ):
        self.tabla = tabla
        self.id = id
        self.tipo_dato = tipo_dato
        self.tipo_dato_2 = tipo_dato_2
        self.tipo_dato_3 = tipo_dato_3
        self.valor = valor
        self.origen = origen
        self.valor_antiguo = valor_antiguo
        self.origen_antiguo = origen_antiguo
        self.fecha = fecha
        self.comentario = comentario
        self.bd = bd
        self.id_carga = id_carga

    def insertar(self, id_carga=None):
        self.id_carga = id_carga or self.id_carga

        query = f"""
            INSERT INTO prisma.{self.tabla} (id, tipo_dato, tipo_dato_2, tipo_dato_3, valor, origen, valor_antiguo, origen_antiguo, fecha, comentario, id_carga)
                VALUES (%(id)s, %(tipo_dato)s, %(tipo_dato_2)s, %(tipo_dato_3)s, %(valor)s, %(origen)s, %(valor_antiguo)s, %(origen_antiguo)s, %(fecha)s, %(comentario)s, %(id_carga)s)
                """
        params = {
            "id": self.id,
            "tipo_dato": self.tipo_dato,
            "tipo_dato_2": self.tipo_dato_2,
            "tipo_dato_3": self.tipo_dato_3,
            "valor": self.valor,
            "origen": self.origen,
            "valor_antiguo": self.valor_antiguo,
            "origen_antiguo": self.origen_antiguo,
            "fecha": self.fecha,
            "comentario": self.comentario,
            "id_carga": self.id_carga,
        }
        self.bd.ejecutarConsulta(query, params)
