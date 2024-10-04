import pytest
from models.colectivo.area import Area
from models.investigador import Investigador
import tests.utils.random_utils as ru

from db.conexion import BaseDatos
from models.colectivo.categoria import Categoria
from models.colectivo.centro import Centro
from models.colectivo.departamento import Departamento
from models.colectivo.grupo import Grupo
from models.colectivo.rama import Rama
from tests.seed.generator import (
    crear_datos_area,
    crear_datos_categoria,
    crear_datos_centro,
    crear_datos_departamento,
    crear_datos_grupo,
    crear_datos_investigador,
    crear_datos_rama,
)


@pytest.fixture(scope="session")
def database():
    db = BaseDatos(
        autocommit=False, keep_connection_alive=True, database=None, test=True
    )
    db.startConnection()
    db.connection.start_transaction()
    yield db
    db.connection.rollback()
    db.connection.close()


@pytest.fixture(scope="session")
def seed_centros(database):
    centros = {}
    while len(centros) < 100:
        attributes = crear_datos_centro()
        if attributes["idCentro"] in centros.keys():
            continue
        centro = Centro(db=database)
        centro.set_attributes(attributes)
        centro.create()
        centros[centro.get_primary_key().value] = attributes

    yield centros


@pytest.fixture(scope="session")
def seed_departamentos(database):
    departamentos = {}
    while len(departamentos) < 500:
        attributes = crear_datos_departamento()
        if attributes["idDepartamento"] in departamentos.keys():
            continue
        departamento = Departamento(db=database)
        departamento.set_attributes(attributes)
        departamento.create()
        departamentos[departamento.get_primary_key().value] = attributes

    yield departamentos


@pytest.fixture(scope="session")
def seed_categorias(database):
    categorias = {}
    while len(categorias) < 300:
        attributes = crear_datos_categoria()
        if attributes["idCategoria"] in categorias.keys():
            continue
        categoria = Categoria(db=database)
        categoria.set_attributes(attributes)
        categoria.create()
        categorias[categoria.get_primary_key().value] = attributes

    yield categorias


@pytest.fixture(scope="session")
def seed_grupos(database):
    grupos = {}
    while len(grupos) < 400:
        attributes = crear_datos_grupo()
        if attributes["idGrupo"] in grupos.keys():
            continue
        grupo = Grupo(db=database)
        grupo.set_attributes(attributes)
        grupo.create()
        grupos[grupo.get_primary_key().value] = attributes

    yield grupos


@pytest.fixture(scope="session")
def seed_ramas(database):

    ramas = {}
    while len(ramas) < 10:
        attributes = crear_datos_rama()
        if attributes["idRama"] in ramas.keys():
            continue
        rama = Rama(db=database)
        rama.set_attributes(attributes)
        rama.create()
        ramas[rama.get_primary_key().value] = attributes

    yield ramas


@pytest.fixture(scope="session")
def seed_areas(database, seed_ramas: dict):
    def get_rama():
        rama = ru.random_element(collection=seed_ramas.values())
        return rama

    areas = {}
    while len(areas) < 100:
        attributes = crear_datos_area()
        if attributes["idArea"] in areas.keys():
            continue
        area = Area(db=database)
        area.set_attributes(attributes)

        rama = get_rama()
        attributes["rama"] = rama
        area.set_component("rama", rama)

        area.create()

        areas[area.get_primary_key().value] = attributes

    yield areas


@pytest.fixture(scope="session")
def seed_investigadores(
    database,
    seed_areas: dict,
    seed_grupos: dict,
    seed_departamentos: dict,
    seed_categorias: dict,
    seed_centros: dict,
):
    def get_area():
        area = ru.random_element(collection=seed_areas.values())
        return area

    def get_grupo():
        grupo = ru.random_element(collection=seed_grupos.values())
        return grupo

    def get_departamento():
        departamento = ru.random_element(collection=seed_departamentos.values())
        return departamento

    def get_categoria():
        categoria = ru.random_element(collection=seed_categorias.values())
        return categoria

    def get_centro():
        centro = ru.random_element(collection=seed_centros.values())
        return centro

    investigadors = {}

    i = 1
    while len(investigadors) < 100:
        attributes = crear_datos_investigador(i)
        if attributes["idInvestigador"] in investigadors.keys():
            continue
        investigador = Investigador(db=database)
        investigador.set_attributes(attributes)

        area = get_area()
        attributes["area"] = area
        investigador.set_component("area", area)
        grupo = get_grupo()
        attributes["grupo"] = grupo
        investigador.set_component("grupo", grupo)
        departamento = get_departamento()
        attributes["departamento"] = departamento
        investigador.set_component("departamento", departamento)
        categoria = get_categoria()
        attributes["categoria"] = categoria
        investigador.set_component("categoria", categoria)
        centro = get_centro()
        attributes["centro"] = centro
        investigador.set_component("centro", centro)

        investigador.create()

        investigadors[investigador.get_primary_key().value] = attributes
        i += 1

    yield investigadors
