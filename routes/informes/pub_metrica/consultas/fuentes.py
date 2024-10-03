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
    "grupo": "gi.idGrupo",
    "instituto": "mi.idInstituto",
    "centro_mixto": "mcm.idCentroMixto",
    "unidad_excelencia": "mue.idUdExcelencia",
    "investigador": "i.idInvestigador",
    "centro": "i.idCentro",
    "area": "i.idArea",
    "doctorado": "pd.idDoctorado",
}

# Diccionario para añadir joins si el tipo de fuente lo requiere
tipo_fuente_to_joins = {
    "instituto": "LEFT JOIN i_miembro_instituto mi ON mi.idInvestigador = i.idInvestigador",
    "grupo": "LEFT JOIN i_grupo_investigador gi ON gi.idInvestigador = i.idInvestigador",
    "doctorado": "LEFT JOIN i_profesor_doctorado pd ON pd.idInvestigador = i.idInvestigador",
    "centro_mixto": "LEFT JOIN i_miembro_centro_mixto mcm ON mcm.idInvestigador = i.idInvestigador",
    "unidad_excelencia": "LEFT JOIN i_miembro_unidad_excelencia mue ON mue.idInvestigador = i.idInvestigador",
}

# Almacena si el dato es int o no para introducirlo en la consultra entre comillas o no
es_int = {
    "departamento": False,
    "grupo": False,
    "instituto": True,
    "centro_mixto": True,
    "unidad_excelencia": True,
    "investigador": False,
    "centro": False,
    "area": False,
    "doctorado": True,
}


def consulta_investigadores(fuentes):
    query = """SELECT i.idInvestigador FROM i_investigador_activo i """

    _conditions = []
    _joins = [
        "LEFT JOIN i_categoria cat ON cat.idCategoria = i.idCategoria",
        "LEFT JOIN i_investigador_excluido ie ON ie.idInvestigador = i.idInvestigador",
    ]

    condition_template = "{column} IN ({value})"

    for fuente in fuentes:
        # Almacenar la columna en una variable
        column = tipo_fuente_to_column.get(fuente)
        joins = tipo_fuente_to_joins.get(fuente)
        value = ",".join(
            (valor if es_int[fuente] else f"'{valor}'" for valor in fuentes[fuente])
        )

        condition = condition_template.format(column=column, value=value)

        if joins:
            _joins.append(joins)

        _conditions.append(condition)

    query += f"{' '.join(_joins)}"
    query += f""" WHERE cat.idCategoria != 'honor' 
                AND (cat.tipo_pp != 'exc' OR ie.excluido = 0)
                AND (ie.excluido IS NULL OR ie.excluido != 1)
                AND ({' OR '.join(_conditions)})"""

    db = BaseDatos()
    result = db.ejecutarConsulta(query, [])
    result = normalize_id_list(result)

    return result


def consulta_publicaciones(investigadores, año_inicio, año_fin):
    query = "SELECT DISTINCT idPublicacion FROM p_publicacion p "

    _conditions = conditions.copy()
    _conditions.append(
        f"""p.idPublicacion IN (
                    SELECT idPublicacion FROM p_autor WHERE idInvestigador IN (
                    {','.join(investigadores)}))"""
    )

    _conditions.append(f"p.agno BETWEEN {año_inicio} AND {año_fin}")

    query += f" WHERE {' AND '.join(_conditions)}"

    db = BaseDatos()
    params = []
    result = db.ejecutarConsulta(query, params)
    result = normalize_id_list(result)

    return result
