import copy
import json
from db.conexion import BaseDatos
from routes.carga.publicacion.scopus.carga import CargaPublicacionScopus
from routes.carga.publicacion.wos.carga import CargaPublicacionWos
from routes.carga.publicacion.openalex.carga import CargaPublicacionOpenalex
from routes.carga.publicacion.crossref.carga import CargaPublicacionCrossref
from routes.carga.publicacion.zenodo.carga import CargaPublicacionZenodo


def test_carga_generico():

    id = "-"
    tipo = {"scopus": "scopus_id", "wos": "wos_id", "openalex": "openalex_id"}

    # CargaPublicacionScopus().carga_publicacion(tipo=tipo.get("scopus"), id=id)
    # CargaPublicacionWos().carga_publicacion(tipo=tipo.get("wos"), id=id)
    # CargaPublicacionOpenalex().carga_publicacion(tipo=tipo.get("openalex"), id=id)
    # CargaPublicacionCrossref().carga_publicacion(tipo="doi", id=id)
    CargaPublicacionZenodo().carga_publicacion(tipo="doi", id=id)
