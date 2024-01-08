import tempfile
from routes.carga.investigador.grupos.config import tablas
from flask import request
from db.conexion import BaseDatos

def carga_sica(files):
    actualizar_sica(files)
    actualizar_grupos()

def actualizar_sica(files):
    for file in files:
        filename: str = file.filename
        table_name = filename.replace(".csv", "").lower()
        assert(filename.endswith(".csv"))
        assert(table_name.upper() in tablas)

        csv_data = file.read().decode('utf-8')
        
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


def actualizar_grupos():
    añadir_grupos()
    actualizar_miembros_grupos()
    actualizar_fecha()

def añadir_grupos():
    query = f"""
    TRUNCATE TABLE prisma.i_grupo;
    INSERT INTO prisma.i_grupo VALUES ('0', 'Sin Grupo US', 'Sin Grupo US', '0', 0, '');
    REPLACE INTO prisma.i_grupo (idGrupo, nombre, acronimo, rama, codigo, institucion)
    SELECT CONCAT(g.RAMA, '-', g.CODIGO),
            g.NOMBRE,
            g.ACRONIMO,
            g.RAMA,
            g.CODIGO,
            i.ENTIDAD
    FROM sica2.t_grupos g
    LEFT JOIN sica2.t_instituciones i ON i.ID_ENTIDAD = g.ID_ENTIDAD
    WHERE g.ID_ENTIDAD = 78 AND g.ESTADO = 'Válido';
    """

    db = BaseDatos(database = None)
    result = db.ejecutarConsulta(query)

    return result

def actualizar_miembros_grupos():
    query = f"""
    UPDATE prisma.i_investigador i
    SET idGrupo = (SELECT
                    CASE WHEN pg.idGrupo IS NOT NULL THEN pg.idGrupo ELSE '0' END
                    FROM (SELECT * FROM prisma.i_investigador) ii
                    LEFT JOIN sica2.t_investigadores si ON si.NUMERO_DOCUMENTO = ii.docuIden
                    LEFT JOIN (SELECT MAX(ID_GRUPO) ID_GRUPO, ID_PERSONAL, FECHA_FIN FROM sica2.t_investigadores_grupo
                        WHERE FECHA_FIN = ''
                        GROUP BY ID_PERSONAL ) ig ON ig.ID_PERSONAL = si.ID_PERSONAL
                    LEFT JOIN sica2.t_grupos g ON g.ID_GRUPO = ig.ID_GRUPO
                    LEFT JOIN prisma.i_grupo pg ON pg.idGrupo =  CONCAT(g.RAMA, '-', g.CODIGO)
                    WHERE ii.idInvestigador = i.idInvestigador)
    """

    db = BaseDatos(database = None)
    result = db.ejecutarConsulta(query)

    return result

def actualizar_fecha():
    query = "UPDATE prisma.a_configuracion SET valor = DATE_FORMAT(NOW(), '%d/%m/%Y') WHERE variable = 'ACTUALIZACION_GRUPOS'"
    
    db = BaseDatos()
    result = db.ejecutarConsulta(query)

    return result