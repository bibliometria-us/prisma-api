import mysql.connector
from mysql.connector.errors import OperationalError
import db.claves as claves
from utils.timing import func_timer as timer


class BaseDatos:

    def __init__(
        self,
        database: str = "prisma",
        local_infile=False,
        keep_connection_alive=False,
        autocommit=True,
        test=False,
    ) -> None:
        self.is_active = False
        self.connection = None
        self.database = database
        self.local_infile = local_infile
        self.autocommit = autocommit
        self.keep_connection_alive = keep_connection_alive
        self.rowcount = 0
        self.error = False
        self.last_id = None
        self.test = test

    def startConnection(self):
        self.connection = mysql.connector.connect(
            host=claves.db_host if not self.test else claves.test_db_host,
            user=claves.db_user if not self.test else claves.test_db_user,
            password=claves.db_password if not self.test else claves.test_db_password,
            database=self.database,
            autocommit=True,
            allow_local_infile=self.local_infile,
        )

        self.is_active = True

    def is_succesful(self) -> bool:
        return self.error == False

    def has_rows(self) -> bool:
        return self.rowcount > 0

    def closeConnection(self):
        self.connection.close()
        self.is_active = False

    def ejecutarConsulta(self, consulta: str, params: str = []):
        if not self.is_active:
            self.startConnection()

        try:
            cursor = self.connection.cursor()
            cursor.execute(consulta, params=params)
            column_names = cursor.column_names
            result = [column_names] + list(cursor.fetchall())
            self.rowcount = cursor.rowcount
            self.last_id = cursor.lastrowid
        except OperationalError as e:
            if self.keep_connection_alive:
                self.startConnection()
                self.ejecutarConsulta(consulta, params)
        except Exception as e:
            self.error = True
            self.rowcount = 0
            cursor.close()
            return str(e.args)

        self.error == False

        if self.autocommit:
            cursor.close()
        if not self.keep_connection_alive:
            self.closeConnection()

        return result
