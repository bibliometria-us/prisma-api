# Funciones que comprueban si el usuario de la sesión cumple alguna característica (pertenecer a un grupo, departamento...)
from db.conexion import BaseDatos
from flask import session


def get_user_from_api_key(api_key: str) -> str:
    db = BaseDatos("api")
    query = "SELECT uvus FROM api_key a WHERE a.api_key = %(api_key)s"
    params = {"api_key": api_key}
    query_result = db.ejecutarConsulta(query, params)

    if len(query_result) > 1:
        return query_result[1][0]
    else:
        return None


def tiene_rol(rol, api_key=None):
    db = BaseDatos("api")

    if not api_key:
        usuario = session["samlUserdata"]["mail"][0].split("@")[0]
    else:
        usuario = get_user_from_api_key(api_key)

    query = """SELECT EXISTS 
    (SELECT 1 FROM permisos WHERE usuario = %(usuario)s AND rol = %(rol)s)"""

    params = {"usuario": usuario, "rol": rol}
    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def es_visor(api_key=None):
    return tiene_rol("visor", api_key=api_key)


def es_admin(api_key=None):
    return tiene_rol("admin", api_key=api_key)


def es_editor(api_key=None):
    return tiene_rol("editor", api_key=api_key)


def pertenece_a_conjunto(tipo, dato, privileged=False, api_key=None):
    tipo_to_func = {
        "departamento": pertenece_a_departamento,
        "grupo": pertenece_a_grupo,
        "instituto": pertenece_a_instituto,
        "centro": pertenece_a_centro,
        "centro_censo": pertenece_a_centro,
        "investigador": es_investigador,
        "centromixto": pertenece_a_centro_mixto,
        "unidadexcelencia": pertenece_a_unidad_excelencia,
        "doctorado": pertenece_a_doctorado,
    }

    func = tipo_to_func.get(tipo)
    return func(
        dato,
        privileged=privileged,
        api_key=api_key,
    )


def es_investigador(investigador, privileged=False, api_key=None):
    db = BaseDatos()

    if not api_key:
        emails = session["samlUserdata"]["mail"]
    else:
        user = get_user_from_api_key(api_key)
        emails = [user + "@us.es"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idInvestigador = %s)"""

    params = [investigador]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_departamento(departamento, privileged=False, api_key=None):
    db = BaseDatos()

    if not api_key:
        emails = session["samlUserdata"]["mail"]
    else:
        user = get_user_from_api_key(api_key)
        emails = [user + "@us.es"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idDepartamento = %s)"""

    params = [departamento]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_grupo(grupo, privileged=False, api_key=None):
    db = BaseDatos()

    if not api_key:
        emails = session["samlUserdata"]["mail"]
    else:
        user = get_user_from_api_key(api_key)
        emails = [user + "@us.es"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i 
                    LEFT JOIN i_grupo_investigador gi ON gi.idInvestigador = i.idInvestigador
                    WHERE i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND gi.idGrupo = %(grupo)s
                    {"AND gi.rol = 'Investigador principal'" if privileged else ""}
                    )"""

    params = {
        "grupo": grupo,
    }

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_instituto(instituto, privileged=False, api_key=None):
    db = BaseDatos()
    emails = session["samlUserdata"]["mail"]

    if not api_key:
        emails = session["samlUserdata"]["mail"]
    else:
        user = get_user_from_api_key(api_key)
        emails = [user + "@us.es"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idInvestigador IN (SELECT idInvestigador FROM i_miembro_instituto 
                    WHERE idInstituto = %s
                    {"AND rol = 'Responsable'" if privileged else ""}
                    ))"""

    params = [instituto]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_centro(centro, privileged=False, api_key=None):
    db = BaseDatos()
    emails = session["samlUserdata"]["mail"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND (i.idCentro = %(idCentro)s) OR i.idCentroCenso = %(idCentro)s)"""

    params = {"idCentro": centro}

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_centro_mixto(centro_mixto, privileged=False, api_key=None):
    db = BaseDatos()
    emails = session["samlUserdata"]["mail"]

    if not api_key:
        emails = session["samlUserdata"]["mail"]
    else:
        user = get_user_from_api_key(api_key)
        emails = [user + "@us.es"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idInvestigador IN (SELECT idInvestigador FROM i_miembro_centro_mixto 
                    WHERE idCentroMixto = %s
                    {"AND rol = 'Responsable'" if privileged else ""}
                    ))"""

    params = [centro_mixto]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_unidad_excelencia(unidad_excelencia, privileged=False, api_key=None):
    db = BaseDatos()
    emails = session["samlUserdata"]["mail"]

    if not api_key:
        emails = session["samlUserdata"]["mail"]
    else:
        user = get_user_from_api_key(api_key)
        emails = [user + "@us.es"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idInvestigador IN (SELECT idInvestigador FROM i_miembro_unidad_excelencia 
                    WHERE idUdExcelencia = %s
                    {"AND rol = 'Responsable'" if privileged else ""}
                    ))"""

    params = [unidad_excelencia]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_doctorado(doctorado, privileged=False, api_key=None):
    db = BaseDatos()
    emails = session["samlUserdata"]["mail"]

    if not api_key:
        emails = session["samlUserdata"]["mail"]
    else:
        user = get_user_from_api_key(api_key)
        emails = [user + "@us.es"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idInvestigador IN (SELECT idInvestigador FROM i_profesor_doctorado 
                    WHERE idDoctorado = %s
                    
                    ))"""

    params = [doctorado]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0
