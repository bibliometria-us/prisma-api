import pytest
import datetime

from db.conexion import BaseDatos
from models.colectivo.area import Area
from models.colectivo.categoria import Categoria
from models.colectivo.centro import Centro
from models.colectivo.departamento import Departamento
from models.colectivo.rama import Rama
from models.investigador import Investigador

attributes = {
    "idInvestigador": 392,
    "nombre": "David",
    "apellidos": "Benavides Cuevas",
    "docuIden": "77585613N",
    "email": "benavides@us.es",
    "fechaContratacion": datetime.date(2002, 9, 30),
    "nacionalidad": "ESPAÑA",
    "sexo": 1,
    "fechaNacimiento": datetime.date(1976, 8, 26),
    "fechaNombramiento": datetime.date(2021, 12, 27),
    "perfilPublico": 1,
    "resumen": "\n",
}

components = {
    "centro": {"idCentro": "0I37", "nombre": "E.T.S. Ingeniería Informática"},
    "departamento": {
        "idDepartamento": "I0A3",
        "nombre": "Lenguajes y Sistemas Informáticos",
    },
    "categoria": {
        "idCategoria": "A0500",
        "nombre": " Catedrático de Universidad",
        "femenino": " Catedrática de Universidad",
    },
    "area": {
        "idArea": 570,
        "nombre": "Lenguajes y Sistemas Informáticos",
    },
    # "grupo": {
    #    "idGrupo": "TIC-258",
    #    "nombre": "Data-centric Computing Research Hub",
    #    "acronimo": "IDEA",
    #    "rol": "Miembro",
    # },
    # "instituto": {
    #    "idInstituto": 5,
    #    "nombre": "Instituto Universitario de Investigación en Ingeniería Informática",
    #    "acronimo": "I3US",
    #    "rol": "Miembro ordinario",
    # },
}

rama_attr = {
    "idRama": "24",
    "nombre": "Ingeniería Informática",
}


updated_attributes = {
    "nombre": "prueba",
    "apellidos": "prueba aaaa",
    "docuIden": "77585615N",
    "email": "bcab@us.es",
    "fechaContratacion": datetime.date(2003, 9, 30),
    "nacionalidad": "ESPAÑÑA",
    "sexo": 0,
    "fechaNacimiento": datetime.date(1976, 8, 27),
    "fechaNombramiento": datetime.date(2022, 12, 27),
    "perfilPublico": 0,
    "resumen": "prueba\n",
}


def create_investigador(database: BaseDatos) -> Investigador:
    centro = Centro(db=database)
    centro.set_attributes(components.get("centro"))
    centro.create()
    departamento = Departamento(db=database)
    departamento.set_attributes(components.get("departamento"))
    departamento.create()
    categoria = Categoria(db=database)
    categoria.set_attributes(components.get("categoria"))
    categoria.create()
    rama = Rama(db=database)
    rama.set_attributes(rama_attr)
    rama.create()
    area = Area(db=database)
    area.set_attributes(components.get("area"))
    area.set_component("rama", rama_attr)
    area.create()

    investigador = Investigador(db=database)
    investigador.set_attributes(attributes)
    investigador.set_components(components)
    investigador.create()

    return investigador


def test_create_and_delete(database):
    investigador = create_investigador(database)
    investigador_dict = investigador.to_dict()

    investigador_prueba = Investigador(db=database)
    investigador_prueba.set_attribute("idInvestigador", 392)
    investigador_prueba.get()

    investigador_prueba_dict = investigador_prueba.to_dict()

    assert investigador_prueba.get_attribute_value("nombre") != None
    assert investigador_dict == investigador_prueba_dict

    investigador.delete()
    investigador.get()
    assert investigador.get_attribute_value("nombre") == None


def test_create_and_update_attributes(database):
    investigador = create_investigador(database)

    for key, updated_value in updated_attributes.items():
        old_value = attributes.get(key)

        investigador.get()
        assert investigador.get_attribute_value(key) == old_value
        assert investigador.get_attribute_value(key) != None

        investigador.update_attribute(key, updated_value)
        investigador.get()
        assert investigador.get_attribute_value(key) != old_value
        assert investigador.get_attribute_value(key) != None
        assert investigador.get_attribute_value(key) == updated_value

    investigador.delete()
    investigador.get()
    assert investigador.get_attribute_value("nombre") == None


def test_create_and_update_single_components(database):
    investigador = create_investigador(database)
