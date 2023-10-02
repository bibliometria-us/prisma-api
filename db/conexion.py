import mysql.connector
import db.claves as claves


class BaseDatos:

    def __init__(self, database: str = "prisma") -> None:
        self.connection = mysql.connector.connect(
            host=claves.db_host,
            user=claves.db_user,
            password=claves.db_password,
            database=database,
            autocommit=True
        )

    def ejecutarConsulta(self, consulta: str, params: str):
        cursor = self.connection.cursor()
        try:
            cursor.execute(consulta, params=params)
            column_names = cursor.column_names
            result = [column_names] + list(cursor.fetchall())
        except Exception as e:
            cursor.close()
            return str(e.args)

        cursor.close()
        return result
