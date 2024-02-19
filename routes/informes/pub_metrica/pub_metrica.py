from routes.informes.pub_metrica.exception.exception import (
    InformeSinInvestigadores,
    InformeSinPublicaciones,
)
from routes.informes.pub_metrica.pdf import generar_pdf
from routes.informes.utils import calcular_autoria_preferente, format_query
from routes.informes.pub_metrica.consultas.fuentes import (
    consulta_investigadores,
    consulta_publicaciones,
)
from routes.informes.pub_metrica.consultas.publicaciones import datos_publicaciones
from routes.informes.pub_metrica.consultas.jif import consulta_jif
from routes.informes.pub_metrica.consultas.sjr import consulta_sjr
from routes.informes.pub_metrica.consultas.idr import consulta_idr
from routes.informes.pub_metrica.consultas.jci import consulta_jci
from routes.informes.pub_metrica.consultas.citescore import consulta_citescore
from routes.informes.pub_metrica.consultas.resumen import datos_resumen


from routes.informes.pub_metrica.misc import get_dict_plantilla_excel
from utils.utils import list_index_map, replace_none_values
from utils.format import (
    dict_to_excel,
    save_excel_to_file,
    add_hyperlinks_to_excel,
    bold_column_titles_excel,
)

from utils.timing import func_timer as timer

# Genera el informe PubMetrica


# @timer
def generar_informe(fuentes, año_inicio, año_fin, tipo, filename):

    investigadores = consulta_investigadores(fuentes)
    if not investigadores:
        raise InformeSinInvestigadores

    publicaciones = consulta_publicaciones(investigadores, año_inicio, año_fin)
    if not publicaciones:
        raise InformeSinPublicaciones(año_inicio, año_fin)

    resumen = datos_resumen(fuentes, investigadores, publicaciones, año_inicio, año_fin)

    if tipo == "excel":
        columnas_publicaciones = datos_publicaciones(investigadores, publicaciones)
        jif = consulta_jif(publicaciones)
        sjr = consulta_sjr(publicaciones)
        idr = consulta_idr(publicaciones)
        citescore = consulta_citescore(publicaciones)

        generar_excel(
            investigadores,
            columnas_publicaciones,
            jif,
            sjr,
            idr,
            citescore,
            filename + ".xlsx",
        )

    elif tipo == "pdf":
        generar_pdf(resumen, filename + ".pdf")

    return None


# @timer
def generar_excel(
    investigadores, columnas_publicaciones, jif, sjr, idr, citescore, filename
):

    result_dict = dict_informe(
        investigadores,
        publicaciones=replace_none_values(columnas_publicaciones),
        jif=replace_none_values(jif),
        sjr=replace_none_values(sjr),
        idr=replace_none_values(idr),
        citescore=replace_none_values(citescore),
    )

    excel = dict_to_excel(result_dict)
    excel = add_hyperlinks_to_excel(excel)
    excel = bold_column_titles_excel(excel)
    save_excel_to_file(excel, filename)


# A partir de los resultados de las consultas, genera un diccionario con una estructura [pagina][publicacion][columna] para luego transformarlo a Excel


def dict_informe(investigadores, publicaciones, jif, sjr, idr, citescore):
    # Cargar mapeo de indices para cada consulta
    pub_indices = list_index_map(publicaciones[0])
    jif_indices = list_index_map(jif[0])
    sjr_indices = list_index_map(sjr[0])
    idr_indices = list_index_map(idr[0])
    citescore_indices = list_index_map(citescore[0])

    # Crear la estructura de diccionarios anidados
    result = get_dict_plantilla_excel()

    # Para cada publicación, rellenar el diccionario con sus datos
    for index, publicacion in enumerate(publicaciones[1:]):

        # Calcular si tiene o no autoría prefetente
        result["Prisma"]["Autoría preferente"][index] = calcular_autoria_preferente(
            investigadores, publicacion[pub_indices["lista_autores"]]
        )

        # Datos de la página Prisma
        for dato in result["Prisma"]:
            if pub_indices.get(dato):
                result["Prisma"][dato][index] = publicacion[pub_indices.get(dato)]

        # Datos de la página Citas
        for dato in result["Citas"]:
            if pub_indices.get(dato):
                result["Citas"][dato][index] = publicacion[pub_indices.get(dato)]

        # Datos de la página JIF
        for dato in result["JIF"]:
            if pub_indices.get(dato):
                result["JIF"][dato][index] = publicacion[pub_indices.get(dato)]
            if jif_indices.get(dato):
                result["JIF"][dato][index] = jif[index + 1][jif_indices.get(dato)]

        # Datos de la página SJR
        for dato in result["SJR"]:
            if pub_indices.get(dato):
                result["SJR"][dato][index] = publicacion[pub_indices.get(dato)]
            if sjr_indices.get(dato):
                result["SJR"][dato][index] = sjr[index + 1][sjr_indices.get(dato)]

        # Datos de la página Dialnet
        for dato in result["Dialnet"]:
            if pub_indices.get(dato):
                result["Dialnet"][dato][index] = publicacion[pub_indices.get(dato)]
            if idr_indices.get(dato):
                result["Dialnet"][dato][index] = idr[index + 1][idr_indices.get(dato)]

        # Datos de la página CiteScore
        for dato in result["CiteScore"]:
            if pub_indices.get(dato):
                result["CiteScore"][dato][index] = publicacion[pub_indices.get(dato)]
            if citescore_indices.get(dato):
                result["CiteScore"][dato][index] = citescore[index + 1][
                    citescore_indices.get(dato)
                ]
    return result
