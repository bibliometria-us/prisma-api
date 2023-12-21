from io import StringIO
import tempfile
from routes.carga.investigador.grupos.config import tablas
from flask import request
from db.conexion import BaseDatos

def actualizar_sica(files):
    for file in files:
        filename: str = file.filename
        table_name = filename.replace(".csv", "").lower()
        assert(filename.endswith(".csv"))
        assert(table_name.upper() in tablas)

        csv_data = file.read().decode('utf-8')
        csv_file = StringIO(csv_data)
        
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name

        db = BaseDatos(database="sica2", local_infile = True)


        query = f"""
            LOAD DATA LOCAL INFILE %s
            INTO TABLE {table_name}
            CHARACTER SET UTF8
            COLUMNS TERMINATED BY ';'
            OPTIONALLY ENCLOSED BY '\"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES;
        """
        
        params = [temp_file_path]

        db.ejecutarConsulta(query, params)