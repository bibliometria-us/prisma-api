import csv
import io
import os
from datetime import datetime
import pandas as pd
from flask import Response, make_response, request, jsonify
from werkzeug.datastructures import FileStorage


def procesado_fichero(file: FileStorage):
    # TODO: Implementar procesado del fichero con todas las funciones secundarias que requiera
    # 1. Transformar a csv (esto hay que verlo, podemos forzar a que el fichero se suba como csv, o admitir xls/xlsx y si fuese, transformarlo a csv)
    base_path = "temp/carga_centros_censo/"
    ext = ".csv"
    df = pd.read_excel(file)

    # Inspeccionar el DataFrame (opcional)
    # 2. Normalizar columnas. Si acabamos con ficheros con columnas con nombres distintos, gestionarlo para que se puedan leer.
    # La idea es que siempre tengamos un fichero final con la misma forma sin importar el input.
    # COLUMNAS:
    # dni: DNI del investigador del centro
    # id_centro: id del centro donde ese investigador está censando
    dni_column = [
        col for col in df.columns if "dni" in col.lower() or "nif" in col.lower()
    ]
    if dni_column:
        # Renombrar la primera coincidencia a "dni"
        df.rename(columns={dni_column[0]: "dni"}, inplace=True)
    id_centro_column = [col for col in df.columns if "centro" in col.lower()]
    if id_centro_column:
        # Renombrar la primera coincidencia a "dni"
        df.rename(columns={id_centro_column[0]: "id_centro"}, inplace=True)

    # 3. Decidir el nombre de fichero, con un formato tipo YYYYMMDDmmss.csv
    # Obtener la fecha y hora actual
    file_name = datetime.now().strftime("%Y%m%d%H%M%S%f")

    # 4. Almacenarlo en temp/carga_centros_censo/<filename>
    file_path = f"{base_path}{file_name}{ext}"
    os.makedirs(base_path, exist_ok=True)
    df.to_csv(file_path)

    # 5. Devolver la ruta del fichero para pasarsela al AsyncRequest como parámetro
    #
    # NOTA: En los pasos de normalización/reconstrucción del fichero, aprovechar para hacer todas las comprobaciones posibles. Por ejemplo,
    # si hay alguna columna mal, un DNI en mal formato/que falte... Y levantar excepciones si se detecta algo, para que se lancen antes de iniciar el proceso.
    return file_path


# Obtener la fecha y hora actual
fecha_hora_actual = datetime.now()

# Formatear la fecha
fecha_formateada = fecha_hora_actual.strftime("%d/%m/%Y")
print(fecha_formateada)
