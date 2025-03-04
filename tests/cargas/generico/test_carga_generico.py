import copy
import json
from db.conexion import BaseDatos
from models.investigador import Investigador
from routes.carga.publicacion.scopus.carga import CargaPublicacionScopus
from routes.carga.publicacion.wos.carga import CargaPublicacionWos
from routes.carga.publicacion.openalex.carga import CargaPublicacionOpenalex
from routes.carga.publicacion.crossref.carga import CargaPublicacionCrossref
from routes.carga.publicacion.zenodo.carga import CargaPublicacionZenodo


def test_carga_generico():

    investigador = Investigador()
    investigadores = investigador.get(all=True)
    pass
