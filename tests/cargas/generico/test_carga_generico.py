import copy
import json
from db.conexion import BaseDatos
from models.investigador import Investigador
from routes.carga.publicacion.scopus.carga import ExtraccionPublicacionScopus
from routes.carga.publicacion.wos.carga import ExtraccionPublicacionWos
from routes.carga.publicacion.openalex.carga import ExtraccionPublicacionOpenalex
from routes.carga.publicacion.crossref.carga import ExtraccionPublicacionCrossref
from routes.carga.publicacion.zenodo.carga import CargaPublicacionZenodo


def test_carga_generico():

    investigador = Investigador()
    investigadores = investigador.get(all=True)
    pass
