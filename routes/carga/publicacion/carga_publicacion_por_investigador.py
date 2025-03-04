from routes.carga import consultas_cargas
from routes.carga.publicacion.openalex.carga import CargaPublicacionOpenalex
from routes.carga.publicacion.scopus.carga import CargaPublicacionScopus
from routes.carga.publicacion.wos.carga import CargaPublicacionWos


def carga_publicaciones_investigador(id_investigador, agno_inicio=None, agno_fin=None):
    identificadores = consultas_cargas.get_id_investigadores(
        id_investigador=id_investigador
    )

    # TODO: REVISAR ORDEN EJECUCIÓN
    for key, value in identificadores.items():
        if value.get("tipo") == "scopus":
            idInves = value.get("valor")
            CargaPublicacionScopus().cargar_publicaciones_por_investigador(
                id_investigador=idInves,
                agno_inicio=agno_inicio,
                agno_fin=agno_fin,
            )
        if value.get("tipo") == "researcherid":
            # CargaPublicacionWos().cargar_publicaciones_por_investigador(
            #     id_investigador=value.get("valor"),
            #     agno_inicio=agno_inicio,
            #     agno_fin=agno_fin,
            # )
            pass
        if value.get("tipo") == "openalex":
            # CargaPublicacionOpenalex().cargar_publicaciones_por_investigador(
            #     id_investigador=value.get("valor"),
            #     agno_inicio=agno_inicio,
            #     agno_fin=agno_fin,
            # )
            pass
