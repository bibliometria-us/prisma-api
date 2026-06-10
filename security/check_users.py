# Funciones que comprueban si el usuario de la sesión cumple alguna característica (pertenecer a un grupo, departamento...)
from db.conexion import BaseDatos
from flask import session


def get_user_from_api_key(api_key: str) -> str:
    if not api_key:
        return

    db = BaseDatos("api")
    query = "SELECT uvus FROM api_key a WHERE a.api_key = %(api_key)s"
    params = {"api_key": api_key}
    query_result = db.ejecutarConsulta(query, params)

    if len(query_result) > 1:
        return query_result[1][0]
    else:
        return None


def get_email_from_api_key(api_key: str) -> str:
    user = get_user_from_api_key(api_key=api_key)

    if user:
        return f"{user}@us.es"

def get_user_roles(user: str):
    db = BaseDatos("api")
    query = """SELECT rol FROM permisos WHERE usuario = %(usuario)s"""
    params = {"usuario": user}
    
    db.ejecutarConsulta(query, params)
    df = db.get_dataframe()
    
    return df["rol"].tolist()

def get_permissions_from_endpoint(endpoint: str, action: str, prefix: str = "api."):
    if endpoint.startswith(prefix):
        endpoint = endpoint[len(prefix):]


    db = BaseDatos("api")
    query = """SELECT rol FROM permisos_endpoint WHERE endpoint = %(endpoint)s AND accion = %(accion)s"""
    params = {"endpoint": endpoint, "accion": action}

    db.ejecutarConsulta(query, params)
    df = db.get_dataframe()
    
    result = df["rol"].tolist()
    result.append(endpoint)
    
    return result

def check_endpoint_permissions(endpoint: str, action: str, user: str, prefix: str = "api."):
    user_roles = get_user_roles(user)
    if "admin" in user_roles:
        return True
    
    endpoint_permissions = get_permissions_from_endpoint(endpoint, action, prefix=prefix)
    if len(endpoint_permissions) == 1:
        return True

    return any(role in user_roles for role in endpoint_permissions)

def tiene_rol(rol, api_key=None):
    db = BaseDatos("api")

    if not api_key:
        saml_user_data = session.get("samlUserdata", {})
        usuario = saml_user_data.get("mail", [""])[0].split("@")[0] or None
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
