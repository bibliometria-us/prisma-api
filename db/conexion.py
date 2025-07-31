import mysql.connector
from mysql.connector.errors import OperationalError
from pandas import DataFrame
import db.claves as claves
from utils.format import table_to_pandas
from utils.timing import func_timer as timer


class BaseDatos:
    """
    Clase que representa un objeto Base de Datos, el cual es necesario para en ciertas funciones
    donde se hace uso de la misma.
    """

    def __init__(
        self,
        database: str = "prisma",
        local_infile=False,
        keep_connection_alive=False,
        autocommit=True,
        test=False,
    ) -> None:
        # Constructor y se inicializan los atributos.
        self.is_active = False  # Indica que la conexión está activa
        self.connection = None  # Representa el objeto de la conexion
        self.database = database
        self.local_infile = local_infile
        self.autocommit = autocommit
        self.keep_connection_alive = keep_connection_alive
        self.rowcount = 0
        self.error = False
        self.last_id = None
        self.test = test
        self.result = None

    def startConnection(self):
        """
        Permite establecer una conexion con la bd.
        """
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
        """
        Devuelve si la consulta contiene filas
        """
        return self.rowcount > 0

    def closeConnection(self):
        """
        Cierra la conexion del objeto de la base de datos (rollback incluido).
        """
        if self.test:
            return None
        self.connection.close()
        self.connection.rollback
        self.is_active = False

    def commit(self):
        if self.test:
            return None

        self.connection.commit()

    def rollback(self):
        """
        Se realiza un rollback.
        """
        self.connection.rollback()

    def set_savepoint(self, savepoint: str):
        """
        Se crea un punto de guardado.
        """
        self.ejecutarConsulta(f"SAVEPOINT {savepoint}")

    def rollback_to_savepoint(self, savepoint: str):
        """
        Se vuelve al punto de guardado.
        """
        self.ejecutarConsulta(f"ROLLBACK TO SAVEPOINT {savepoint}")

    def ejecutarConsulta(self, consulta: str, params: str = []):
        """
        Ejecuta una consuta en la conexion de la base de datos.
        """
        if not self.is_active:
            self.startConnection()

        try:
            cursor = self.connection.cursor()
            cursor.execute(consulta, params=params)
            column_names = cursor.column_names
            result = [column_names] + list(cursor.fetchall())
            self.rowcount = cursor.rowcount
            self.last_id = cursor.lastrowid
        except OperationalError as e:  # DUDA: que se controla con esta excepción?
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

        self.result = result
        return result

    def get_first_cell(self):
        """
        Obtiene la primera celda si existe en el resultado de la consulta.
        """
        if not (self.result and len(self.result)) > 1:
            return None

        return self.result[1][0]

    def get_dataframe(self) -> DataFrame:
        """
        Obtienes el resultado de la consulta como un DataFrame
        """
        if not self.result:
            return DataFrame()

        return table_to_pandas(self.result)

    def consultaUna(self, consulta: str, params: tuple = ()):
        """
        Ejecuta una consulta y devuelve la primera fila como un diccionario
        con claves = nombres de columnas y valores = datos.
        Si no hay resultados, devuelve None.
        """
        resultado = self.ejecutarConsulta(consulta, params)

        if self.rowcount == 0 or not resultado or len(resultado) < 2:
            return None

        columnas = resultado[0]  # Primera fila: nombres de columna
        valores = resultado[1]  # Segunda fila: primer registro de datos

        return dict(zip(columnas, valores))
