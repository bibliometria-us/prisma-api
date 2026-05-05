from abc import ABC, abstractmethod
import pandas as pd
import os


class Reader(ABC):
    """
    Clase abstracta para la lectura de archivos de texto (CSV, Excel, etc.).
    Devuelve los datos como una lista de diccionarios.
    """

    def __init__(self, file_path, expected_columns=None):
        """
        Inicializa la clase con la ruta del archivo y las columnas esperadas.

        :param file_path: str - Ruta del archivo a leer.
        :param expected_columns: list - Lista de nombres de columnas esperadas (opcional).
        """
        self.file_path = file_path
        self.expected_columns = expected_columns

    def set_path(self, file_path):
        """
        Establece la ruta del archivo a leer.
        """
        self.file_path = file_path

    def set_expected_columns(self, expected_columns):
        """
        Establece las columnas esperadas en el archivo.
        """
        self.expected_columns = expected_columns

    def validate_file(self):
        """
        Verifica si el archivo existe.
        """
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Error: El archivo '{self.file_path}' no existe.")
        return True

    def validate_columns(self, df, nombre_hoja=None):
        """
        Valida si el DataFrame contiene las columnas esperadas.
        """
        if self.expected_columns:
            missing_columns = set(self.expected_columns) - set(df.columns)
            if missing_columns:
                raise ValueError(
                    f"Faltan columnas {missing_columns} en la hoja '{nombre_hoja}' del archivo."
                )
        return True

    @abstractmethod
    def read(self, sheet_name=0, skiprows=0) -> list[dict]:
        """
        Método abstracto que debe implementarse en cada subclase para leer un archivo.
        Debe devolver una lista de diccionarios.
        """
        pass
