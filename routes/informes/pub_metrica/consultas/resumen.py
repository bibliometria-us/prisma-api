from utils.timing import func_timer as timer
from db.conexion import BaseDatos
from routes.informes.utils import calcular_autoria_preferente
from utils.format import dict_from_table
from utils.utils import list_index_map, replace_none_values
import routes.informes.config as config
import copy
# Dada una lista de investigadores y sus publicaciones, obtener un resumen de los datos


# @timer
def datos_resumen(fuentes, investigadores, publicaciones, año_inicio, año_fin):
    result = {}

    result["año_inicio"] = año_inicio
    result["año_fin"] = año_fin
    result["cantidad_investigadores"] = len(investigadores)
    result["titulo"] = nombres_fuentes(fuentes)
    result["numero_miembros"] = len(investigadores)
    result["investigadores_por_rama"] = investigadores_por_rama(investigadores)
    result["investigadores_por_categoria"] = investigadores_por_categoria(
        investigadores)
    result["publicaciones_por_año"] = publicaciones_por_año(
        publicaciones)
    result["publicaciones_por_tipo"] = publicaciones_por_tipo(
        publicaciones)
    result["publicaciones_por_autoria"] = publicaciones_por_autoria(
        investigadores, publicaciones)
    result["citas_publicaciones"] = citas_publicaciones(
        publicaciones, investigadores, año_inicio, año_fin)
    result["distribucion_publicaciones"] = distribucion_publicaciones(
        publicaciones)

    return result


def nombres_fuentes(fuentes):

    query = "SELECT {columna} FROM {tabla} WHERE {condicion}"

    result = {}

    for fuente in fuentes:
        tipo_fuente = fuente
        valor_fuente = fuentes[tipo_fuente]

        db = BaseDatos()

        if tipo_fuente == "departamento":
            _query = query.format(**{
                "columna": "nombre",
                "tabla": "i_departamento",
                "condicion": f"idDepartamento = '{valor_fuente}'"
            })

        if tipo_fuente == "grupo":
            _query = query.format(**{
                "columna": "nombre",
                "tabla": "i_grupo",
                "condicion": f"idGrupo = '{valor_fuente}'"
            })

        if tipo_fuente == "instituto":
            _query = query.format(**{
                "columna": "nombre",
                "tabla": "i_instituto",
                "condicion": f"idInstituto = '{valor_fuente}'"
            })

        if tipo_fuente == "investigadores":
            _query = query.format(**{
                "columna": "CONCAT(apellidos, ', ', nombre)",
                "tabla": "i_investigador",
                "condicion": f"idInvestigador IN ({(',').join(valor_fuente)})"
            })

        if tipo_fuente == "centro":
            _query = query.format(**{
                "columna": "nombre",
                "tabla": "i_centro",
                "condicion": f"idCentro = '{valor_fuente}'"
            })

        datos = db.ejecutarConsulta(_query)

        if tipo_fuente != "investigadores":
            result[tipo_fuente] = datos[1][0]
        else:
            result[tipo_fuente] = list([dato[0] for dato in datos[1:]])

    return result


def investigadores_por_rama(investigadores):
    query = f"""
            SELECT
            CONCAT(rama.idRama,'_' ,area.idArea) as 'id',
            rama.nombre as 'Rama US',
            area.nombre as 'Área de Conocimiento',
            COUNT(DISTINCT i.idInvestigador) as 'Miembros'
            FROM i_investigador_activo i
            LEFT JOIN i_rama_us ramaus ON ramaus.idDepartamento = i.idDepartamento AND ramaus.idArea = i.idArea
            LEFT JOIN i_rama rama ON rama.idRama = ramaus.idRama
            LEFT JOIN i_area area ON ramaus.idArea = area.idArea
            WHERE idInvestigador IN ({','.join(investigadores)})
            GROUP BY rama.nombre, area.nombre
            ORDER BY COUNT(DISTINCT i.idInvestigador) DESC
            """

    db = BaseDatos()
    result = db.ejecutarConsulta(query)

    result = replace_none_values(result)
    result = dict_from_table(
        data=result, selectable_column="id")
    return result


def investigadores_por_categoria(investigadores):
    query = f"""
            SELECT
            c.idCategoria as 'id',
            c.nombre as 'Categoría Profesional',
            COUNT(DISTINCT i.idInvestigador) as 'Miembros'
            FROM i_investigador_activo i
            LEFT JOIN i_categoria c ON c.idCategoria = i.idCategoria
            WHERE i.idInvestigador IN ({','.join(investigadores)})
            GROUP BY c.nombre, c.idCategoria
            ORDER BY COUNT(DISTINCT i.idInvestigador) DESC, c.nombre
            """

    db = BaseDatos()
    result = db.ejecutarConsulta(query)

    result = replace_none_values(result)
    result = dict_from_table(
        data=result, selectable_column="id")
    return result


def publicaciones_por_año(publicaciones):
    query = f"""
            SELECT
            p.agno as 'Año',
            COUNT(p.idPublicacion) as 'Nº de publicaciones'
            FROM p_publicacion p
            WHERE p.idPublicacion IN ({','.join(publicaciones)})
            GROUP BY p.agno
            ORDER BY p.agno            
            """

    db = BaseDatos()
    result = db.ejecutarConsulta(query)

    result = replace_none_values(result)
    result = dict_from_table(
        data=result, selectable_column="Año")
    return result


def publicaciones_por_tipo(publicaciones):
    query = f"""
            SELECT
            p.tipo as 'Tipo',
            COUNT(p.idPublicacion) as 'Nº de publicaciones'
            FROM p_publicacion p
            WHERE p.idPublicacion IN ({','.join(publicaciones)})
            GROUP BY p.tipo
            ORDER BY COUNT(p.idPublicacion) DESC 
            """

    db = BaseDatos()
    result = db.ejecutarConsulta(query)

    result = replace_none_values(result)
    result = dict_from_table(
        data=result, selectable_column="Tipo")
    return result


def publicaciones_por_autoria(investigadores, publicaciones):
    query = f"""
            SELECT
            COUNT(autor.idAutor) as 'cantidad_autores',
            COUNT(autor_inf.idAutor) as 'autores_informe',
            COUNT(DISTINCT CASE WHEN autor_inf.rol = 'Grupo' THEN autor_inf.idAutor ELSE NULL END) as 'autores_informe_grupales',
            GROUP_CONCAT(DISTINCT CASE WHEN autor.rol != 'Grupo' THEN CONCAT(autor.idInvestigador, ',' , autor.orden, ',', autor.contacto) ELSE NULL END SEPARATOR ';') as 'lista_autores'
            FROM p_publicacion p
            LEFT JOIN p_autor autor ON autor.idPublicacion = p.idPublicacion
            LEFT JOIN (SELECT * FROM p_autor WHERE idInvestigador IN ({','.join(investigadores)})) autor_inf ON autor_inf.idAutor = autor.idAutor
            WHERE p.idPublicacion IN ({','.join(publicaciones)})
            GROUP BY p.idPublicacion
            """

    db = BaseDatos()
    datos = db.ejecutarConsulta(query)
    result = {
        "Autoría grupal exclusiva": 0,
        "Autoría preferente": 0,
        "A partir de 30 autores": 0,
        "A partir de 100 autores": 0,
    }
    for row in datos[1:]:
        cantidad_autores = row[0]
        cantidad_autores_informe = row[1]
        cantidad_autores_informe_grupales = row[2]
        lista_autores = row[3]

        if cantidad_autores >= 30:
            result["A partir de 30 autores"] += 1
        if cantidad_autores >= 100:
            result["A partir de 100 autores"] += 1
        if cantidad_autores_informe == cantidad_autores_informe_grupales:
            result["Autoría grupal exclusiva"] += 1
        if calcular_autoria_preferente(investigadores, lista_autores) == "Sí":

            result["Autoría preferente"] += 1

    return result


def citas_publicaciones(publicaciones, investigadores, año_inicio, año_fin):
    rango_años = [i for i in range(año_inicio, año_fin + 1)]

    query = f"""
            SELECT
            COALESCE(MAX(citas.valor), 0) as citas,
            CASE WHEN (
            COUNT(autor_inf.idAutor) - COUNT(DISTINCT CASE WHEN autor_inf.rol = 'Grupo' THEN autor_inf.idAutor ELSE NULL END)
            ) = 0 THEN 'Sí' ELSE 'No' END as 'autoria_grupal_exclusiva',
            CASE WHEN MAX(idpub.idPublicacion) IS NULL THEN FALSE ELSE TRUE END as 'tiene_identificador', 
            p.agno as año
            FROM p_publicacion p
            LEFT JOIN (SELECT idPublicacion, 
                        CAST(valor AS SIGNED INTEGER) as valor
                        FROM m_publicaciones 
                        WHERE metrica = 'citas' AND basedatos = 'DB_PLACEHOLDER') citas 
                        ON citas.idPublicacion = p.idPublicacion
            LEFT JOIN (SELECT idPublicacion, valor
                        FROM p_identificador_publicacion
                        WHERE tipo = 'DB_PLACEHOLDER') idpub ON idpub.idPublicacion = p.idPublicacion
            LEFT JOIN p_autor autor ON autor.idPublicacion = p.idPublicacion
            LEFT JOIN (SELECT * FROM p_autor WHERE idInvestigador IN ({','.join(investigadores)})) autor_inf ON autor_inf.idAutor = autor.idAutor
            WHERE p.idPublicacion IN ({','.join(publicaciones)})
            GROUP BY p.idPublicacion
            ORDER BY MIN(citas.valor) DESC
            """

    db = BaseDatos()

    # Obtener datos de scopus y WOS
    datos_scopus = db.ejecutarConsulta(
        query.replace("DB_PLACEHOLDER", "scopus"))
    datos_wos = db.ejecutarConsulta(
        query.replace("DB_PLACEHOLDER", "wos"))

    # Diccionario plantilla para almacenar las métricas de cada base de datos
    metricas = {
        "cantidad_publicaciones": 0,
        "citas": 0,
        "media_citas": 0,
        "indice_h": 0
    }
    grupos = {
        "publicaciones": copy.deepcopy(metricas),
        "publicaciones_autoria_grupal": copy.deepcopy(metricas),
    }
    bds_metricas = {
        "scopus":
            copy.deepcopy(grupos),
        "wos":
            copy.deepcopy(grupos)
    }

    # Diccionario resultado
    result = {
        "Total":
            copy.deepcopy(bds_metricas),
    }

    # Al diccionario resultado, añadimos una entrada por cada año
    for año in rango_años:
        result[año] = copy.deepcopy(bds_metricas)

    # Con la plantilla del resultado completa, rellenamos datos
    def rellenar_resultados(result, datos, basedatos):

        # Mapeo de indices para las columnas de la tabla de datos
        indices_tabla = list_index_map(datos[0])
        for publicacion in datos[1:]:
            año = int(publicacion[indices_tabla["año"]])
            autoria_grupal_exclusiva = publicacion[indices_tabla["autoria_grupal_exclusiva"]]
            # Calculamos donde introducir el resultado en base a si es o no autoría grupal exclusiva
            aut_gr_ex = "publicaciones" if autoria_grupal_exclusiva == "No" else "publicaciones_autoria_grupal"
            citas = publicacion[indices_tabla["citas"]]
            # Se tiene en cuenta si tiene o no identificador para contar o no la publicación
            tiene_identificador = publicacion[indices_tabla["tiene_identificador"]]

            if tiene_identificador:
                datos_total = result["Total"][basedatos][aut_gr_ex]
                datos_año = result[año][basedatos][aut_gr_ex]
                # Sumar cantidad de publicaciones
                datos_total["cantidad_publicaciones"] += 1
                datos_año["cantidad_publicaciones"] += 1

                # Sumar cantidad de citas
                datos_total["citas"] += citas
                datos_año["citas"] += citas

                # Actualizar indices h
                if citas >= datos_total["indice_h"]:
                    datos_total["indice_h"] += 1

                if citas >= datos_año["indice_h"]:
                    datos_año["indice_h"] += 1

                # Actualizar media
                datos_total["media_citas"] = str(round(datos_total["citas"] /
                                                       datos_total["cantidad_publicaciones"], 2)).replace(".", ",")
                datos_año["media_citas"] = str(round(datos_año["citas"] /
                                                     datos_año["cantidad_publicaciones"], 2)).replace(".", ",")

    rellenar_resultados(result, datos_scopus, "scopus")
    rellenar_resultados(result, datos_wos, "wos")

    return result


def distribucion_publicaciones(publicaciones):
    query = f"""
        SELECT
        MIN(jif.quartile) AS cuartil_jif,
        MIN(jif.decil) AS decil_jif,
        MIN(sjr.quartile) AS cuartil_sjr,
        MIN(sjr.decil) AS decil_sjr
        FROM p_publicacion p
        LEFT JOIN m_jcr jif ON jif.idFuente = p.idFuente AND jif.year = LEAST(p.agno, {config.max_jcr_year})
        LEFT JOIN m_sjr sjr ON sjr.idFuente = p.idFuente AND sjr.year = LEAST(p.agno, {config.max_sjr_year})
        WHERE p.idPublicacion IN ({','.join(publicaciones)})
        GROUP BY p.idPublicacion
            """

    db = BaseDatos()
    datos = db.ejecutarConsulta(query)

    # Diccionarios plantilla de cuartiles y deciles para el resultado
    atributos_metrica = {"valor": 0, "porcentaje": 0}
    dict_cuartiles = {"incluidas": {f"Q{i}": copy.deepcopy(atributos_metrica) for i in range(1, 4 + 1)} |
                      {"total": 0},
                      "no_incluidas": 0}
    dict_deciles = {"incluidas": {f"D{i}": copy.deepcopy(atributos_metrica) for i in range(1, 10 + 1)} |
                    {"total": 0},
                    "no_incluidas": 0}

    dict_metricas = {"cuartiles": copy.deepcopy(dict_cuartiles),
                     "deciles": copy.deepcopy(dict_deciles), }

    lista_metricas = ("JIF", "SJR")

    result = {metrica: copy.deepcopy(dict_metricas)
              for metrica in lista_metricas}

    # Rellenar la tabla de resultados

    indices_tabla = list_index_map(datos[0])
    for publicacion in datos[1:]:
        metricas = {
            "cuartil_jif": publicacion[indices_tabla["cuartil_jif"]],
            "decil_jif": publicacion[indices_tabla["decil_jif"]],
            "cuartil_sjr": publicacion[indices_tabla["cuartil_sjr"]],
            "decil_sjr": publicacion[indices_tabla["decil_sjr"]],
        }

        for metrica in metricas:
            # Cogemos el título de la métrica y lo separamos por _ para obtener el tipo de métrica y la bd correspondiente
            datos_metrica = metrica.split("_")
            tipo_metrica = datos_metrica[0]
            db_metrica = datos_metrica[1]

            valor = metricas[metrica]

            # Diccionario que sustituye tipos de métricas/bases de datos por las claves requeridas en el diccionario de resultados
            dato_a_clave = {
                "cuartil": "cuartiles",
                "decil": "deciles",
                "jif": "JIF",
                "sjr": "SJR",
            }

            if valor:
                entrada = result[dato_a_clave[db_metrica]
                                 ][dato_a_clave[tipo_metrica]]["incluidas"]
                entrada[valor]["valor"] += 1
                entrada["total"] += 1
                entrada[valor]["porcentaje"] = (str(round(
                    (entrada[valor]["valor"] / entrada["total"]) * 100, 2)) + "%").replace(".", ",")

            else:
                result[dato_a_clave[db_metrica]
                       ][dato_a_clave[tipo_metrica]]["no_incluidas"] += 1

    return result
