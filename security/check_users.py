# Funciones que comprueban si el usuario de la sesión cumple alguna característica (pertenecer a un grupo, departamento...)
from db.conexion import BaseDatos
from flask import session


def es_admin():
    db = BaseDatos("api")

    usuario = session["samlUserdata"]["mail"][0].split("@")[0]
    query = (
        "SELECT EXISTS (SELECT 1 FROM permisos WHERE usuario = %s AND rol = 'admin')"
    )

    params = [usuario]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_conjunto(tipo, dato):
    tipo_to_func = {
        "departamento": pertenece_a_departamento,
        "grupo": pertenece_a_grupo,
        "instituto": pertenece_a_instituto,
        "centro": pertenece_a_centro,
    }

    func = tipo_to_func.get(tipo)
    return func(dato)


def pertenece_a_departamento(departamento):
    db = BaseDatos()
    emails = session["samlUserdata"]["mail"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idDepartamento = %s)"""

    params = [departamento]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_grupo(grupo):
    db = BaseDatos()
    emails = session["samlUserdata"]["mail"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idGrupo = %s)"""

    params = [grupo]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_instituto(instituto):
    db = BaseDatos()
    emails = session["samlUserdata"]["mail"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idInvestigador IN (SELECT idInvestigador FROM i_miembro_instituto WHERE idInstituto = %s))"""

    params = [instituto]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0


def pertenece_a_centro(centro):
    db = BaseDatos()
    emails = session["samlUserdata"]["mail"]

    query = f"""SELECT EXISTS (SELECT 1 FROM i_investigador_activo i WHERE
                    i.email IN ({', '.join(["'{}'".format(email) for email in emails])})
                    AND i.idCentro = %s)"""

    params = [centro]

    result = db.ejecutarConsulta(query, params)[1][0]

    return result != 0
