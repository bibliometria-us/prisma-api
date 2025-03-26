from integration.readers.reader import Reader
import pandas as pd


class RRHHReader(Reader):
    """
    Implementación del Reader para archivos Excel.
    """

    def read(self, sheet_name=0, skiprows=0) -> list[dict]:
        """
        Lee un archivo Excel y devuelve una lista de diccionarios.

        :param sheet_name: str o int - Nombre o índice de la hoja.
        :return: list[dict]
        """

        # TODO: Validar columnas
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, skiprows=skiprows)
        if not self.validate_columns(df, nombre_hoja=sheet_name):
            raise FileNotFoundError(
                f"Error: Las columnas del archivo no son correctas."
            )
        return df.to_dict(orient="records")  # Devuelve lista de diccionarios
