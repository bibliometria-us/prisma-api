from utils.timing import func_timer as timer
from db.conexion import BaseDatos
from routes.informes.utils import normalize_id_list

# Condiciones que tienen que cumplir las publicaciones
conditions = [
    "p.eliminado = 0",
    "p.validado > 1",
    "p.tipo != 'Tesis'",
]

# Mapea el tipo de fuente a la columna correspondiente en la base de datos
tipo_fuente_to_column = {
    "departamento": "i.idDepartamento",
    "grupo": "i.idGrupo",
    "instituto": "mi.idInstituto",
    "investigadores": "i.idInvestigador",
}

# Diccionario para añadir joins si el tipo de fuente lo requiere
tipo_fuente_to_joins = {
    "instituto": "LEFT JOIN i_miembro_instituto mi ON mi.idInvestigador = i.idInvestigador"
}

# Almacena si el dato es int o no para introducirlo en la consultra entre comillas o no
es_int = {
    "departamento": False,
    "grupo": False,
    "instituto": True,
    "investigadores": False
}


def consulta_investigadores(fuentes):
    query = "SELECT i.idInvestigador FROM i_investigador_activo i "

    _conditions = []
    _joins = []
    condition_template = "{column} = {value}"

    for fuente in fuentes:
        # Almacenar la columna en una variable
        column = tipo_fuente_to_column.get(fuente)
        joins = tipo_fuente_to_joins.get(fuente)
        value = fuentes[fuente]

        if not isinstance(value, list):
            # Si el valor no es un entero, se pone entre comillas
            if not es_int[fuente]:
                value = f"'{value}'"
            condition = condition_template.format(
                column=column, value=value)
            if joins:
                _joins.append(joins)

        # Si es una lista de investigadores
        else:
            condition = f"i.idInvestigador IN ({','.join(value)})"
        _conditions.append(condition)

    query += f"{' '.join(_joins)}"
    query += f" WHERE {' AND '.join(_conditions)}"

    db = BaseDatos()
    result = db.ejecutarConsulta(query, [])
    result = normalize_id_list(result)

    return result


def consulta_publicaciones(investigadores, año_inicio, año_fin):
    query = "SELECT DISTINCT idPublicacion FROM p_publicacion p "

    _conditions = conditions.copy()
    _conditions.append(f"""p.idPublicacion IN (
                    SELECT idPublicacion FROM p_autor WHERE idInvestigador IN (
                    {','.join(investigadores)}))""")

    _conditions.append(f"p.agno BETWEEN {año_inicio} AND {año_fin}")

    query += f" WHERE {' AND '.join(_conditions)}"

    db = BaseDatos()
    params = []
    result = db.ejecutarConsulta(query, params)
    result = normalize_id_list(result)

    return result
