import datetime
import tests.utils.random_utils as ru


def crear_datos_centro():
    attributes = {
        "idCentro": ru.random_str(
            4,
            5,
            charset=["uppercase", "digits"],
        ),
        "nombre": ru.random_str(10, 100, capitalize=True, space=True),
    }

    return attributes


def crear_datos_departamento():
    attributes = {
        "idDepartamento": ru.random_str(
            4,
            4,
            charset=["uppercase", "digits"],
        ),
        "nombre": ru.random_str(10, 150, capitalize=True, space=True),
    }

    return attributes


def crear_datos_categoria():
    attributes = {
        "idCategoria": ru.random_str(
            2,
            8,
            charset=["uppercase", "digits", "punctuation"],
        ),
        "nombre": ru.random_str(10, 50, capitalize=True, space=True),
        "femenino": ru.random_str(10, 50, capitalize=True, space=True),
    }

    return attributes


def crear_datos_area():
    attributes = {
        "idArea": ru.random_int(111, 999),
        "nombre": ru.random_str(10, 150, capitalize=True, space=True),
    }

    return attributes


def crear_datos_rama():
    attributes = {
        "idRama": ru.random_int(1, 99),
        "nombre": ru.random_str(10, 150, capitalize=True, space=True),
    }

    return attributes


def crear_datos_grupo():
    attributes = {
        "nombre": ru.random_str(10, 200, space=True, charset=["uppercase"]),
        "acronimo": ru.random_str(10, 75, charset=["uppercase"]),
        "rama": ru.random_str(2, 4, charset=["uppercase"]),
        "codigo": ru.random_int(1, 9999),
        "institucion": ru.random_str(10, 200, space=True),
        "fecha_creacion": ru.random_date(datetime.date(2000, 1, 1)),
        "ambito": ru.random_str(10, 100, space=True),
        "resumen": ru.random_str(10, 3000, space=True),
        "estado": ru.random_element(["Válido", "No Válido"]),
    }

    attributes["idGrupo"] = f"{attributes['rama']}-{str(attributes['codigo'])}"

    return attributes


def crear_datos_investigador(idInvestigador: int):
    attributes = {
        "idInvestigador": idInvestigador,
        "nombre": ru.random_str(5, 15, capitalize=True),
        "apellidos": f"{ru.random_str(5, 15, capitalize=True)} {ru.random_str(5, 15, capitalize=True)}",
        "docuIden": ru.random_dni(),
        "email": f"{ru.random_str(4,20)}@us.es",
        "fechaContratacion": ru.random_date(datetime.date(1970, 1, 1)),
        "nacionalidad": "ESPAÑA",
        "sexo": ru.random_bool_int(),
        "fechaNacimiento": ru.random_date(datetime.date(1930, 1, 1)),
        "perfilPublico": ru.random_bool_int(),
        "resumen": ru.random_str(100, 3000, space=True),
    }

    attributes["fechaNombramiento"] = ru.random_date(attributes["fechaContratacion"])

    return attributes
