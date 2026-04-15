import os
from datetime import datetime
import pandas as pd
from werkzeug.datastructures import FileStorage


def procesado_fichero_erasmus_plus(file: FileStorage) -> str:
    """
    Guarda el fichero Excel recibido para Erasmus+,
    valida que tenga las tres pestañas necesarias y devuelve la ruta local.
    """

    # 1. Definir el directorio temporal para Erasmus+
    base_path = "temp/carga_erasmus_plus/"
    os.makedirs(base_path, exist_ok=True)

    # 2. Construir un nombre único para el fichero basado en fecha/hora
    _, ext = os.path.splitext(file.filename)
    if ext.lower() not in [".xls", ".xlsx"]:
        raise ValueError("Extensión de archivo no válida. Solo se admiten .xls o .xlsx")

    file_name = datetime.now().strftime("%Y%m%d%H%M%S%f") + ext
    file_path = os.path.join(base_path, file_name)

    # 3. Guardar el fichero físicamente
    file.save(file_path)

    # 4. Validación mínima: comprobar que el Excel contiene las 3 hojas requeridas
    try:
        xls = pd.ExcelFile(file_path)
        hojas = xls.sheet_names
        requeridas = {"Proyectos", "Participantes", "Instituciones"}
        if not requeridas.issubset(set(hojas)):
            raise ValueError(
                f"El archivo debe contener exactamente estas hojas: {requeridas}. "
                f"Hojas encontradas: {hojas}"
            )
    except Exception as e:
        # Borrar el archivo si está corrupto o no válido
        if os.path.exists(file_path):
            os.remove(file_path)
        raise ValueError(f"Error al procesar el archivo Excel: {e}")

    # 5. Si todo es correcto, devolver la ruta
    print(f"Archivo Erasmus+ guardado y validado: {file_path}")
    return file_path
