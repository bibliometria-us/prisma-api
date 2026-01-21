from pandas import DataFrame
from db.conexion import BaseDatos
from integration.email.email import enviar_correo
import routes.carga.financiacion.proyecto as proyecto
import routes.carga.financiacion.contrato as contrato
import routes.carga.financiacion.componente as componente


def carga_proyectos(files: dict[str, DataFrame] = None):
    email = "bibliometria@us.es"
    ficheros = []
    try:
        comprobar_ficheros(files)

        # Filtrar y cargar proyectos
        projects = files.get("projects")
        contracts = files.get("contracts")
        components = files.get("components")

        result_carga_proyectos = proyecto.cargar_proyectos(projects=projects)
        result_carga_contratos = contrato.cargar_contratos(contratos=contracts)
        result_carga_componentes = componente.cargar_componentes(componentes=components)
        data = {
            "carga_proyectos": result_carga_proyectos,
            "carga_contratos": result_carga_contratos,
            "carga_componentes": result_carga_componentes,
        }

        # Calcular estadísticas
        estadisticas_proyectos = calcular_estadisticas_proyectos(
            resultados=result_carga_proyectos
        )
        estadisticas_contratos = calcular_estadisticas_miembros(
            resultados=result_carga_contratos
        )
        estadisticas_componentes = calcular_estadisticas_miembros(
            resultados=result_carga_componentes
        )

        errores = (
            estadisticas_proyectos["errores"]
            + estadisticas_contratos["errores"]
            + estadisticas_componentes["errores"]
        )

        inserciones_proyectos = estadisticas_proyectos["inserciones"]

        altas_miembros = (
            estadisticas_componentes["altas"] + estadisticas_contratos["altas"]
        )
        bajas_miembros = (
            estadisticas_componentes["bajas"] + estadisticas_contratos["bajas"]
        )

        mensaje = f"""
        Se han insertado {inserciones_proyectos} proyectos nuevos.
        <br>
        Se han dado de baja {bajas_miembros} miembros de proyectos y se han dado de alta {altas_miembros}.
        {'<br>' + 
         f'Se han detectado {errores} en la carga. Se recomienda revisar los archivos de log para más detalles.' if errores else ''}
        """

        for key, lines in data.items():
            with open(f"temp/{key}.log", "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
                ficheros.append(f.name)

        enviar_correo(
            adjuntos=ficheros,
            asunto="Carga de proyectos",
            destinatarios=[email],
            texto_plano="",
            texto_html=mensaje,
        )

    except Exception as e:
        enviar_correo(
            adjuntos=ficheros,
            asunto="Error: Carga de proyectos",
            destinatarios=[email],
            texto_plano=str(e),
            texto_html=str(e),
        )


def calcular_estadisticas_proyectos(resultados: list[str]):
    errores = 0
    actualizaciones = 0
    inserciones = 0
    for resultado in resultados:
        if "Error insertando el proyecto" in resultado:
            errores += 1
            continue
        if "Error actualizando el atributo" in resultado:
            errores += 1
            continue
        if "Actualizado atributo" in resultado:
            actualizaciones += 1
            continue
        if "Insertado el proyecto" in resultado:
            inserciones += 1
            continue

    return {
        "errores": errores,
        "actualizaciones": actualizaciones,
        "inserciones": inserciones,
    }


def calcular_estadisticas_miembros(resultados: list[str]):
    altas = 0
    bajas = 0
    errores = 0

    for resultado in resultados:
        if "Dado de baja" in resultado:
            bajas += 1
            continue
        if "Dado de alta" in resultado:
            altas += 1
            continue
        if "Error" in resultado:
            errores += 1
            continue

    return {
        "altas": altas,
        "bajas": bajas,
        "errores": errores,
    }


def comprobar_ficheros(files: dict[str, DataFrame]) -> None:
    columns: dict[str, set[str]] = {
        "projects": {
            "Modalidad",
            "IdProyecto",
            "Titulo",
            "Referencia",
            "Organica",
            "FechaInicio",
            "FechaFin",
            "Ambito",
            "ImporteConcedido",
            "ImporteSolicitado",
            "TipoProyecto",
            "Financiador",
            "EsCompetitivo",
        },
        "components": {
            "IdProyecto",
            "NIF",
            "ORCId",
            "Apellidos",
            "Nombre",
            "ParticipaComo",
        },
        "contracts": {
            "IdContrato",
            "NIF",
            "Apellidos",
            "Nombre",
            "Organica",
            "Referencia",
            "FechaInicio",
            "FechaFin",
            "FechaRenuncia",
        },
    }

    for name, column_set in columns.items():
        missing_columns = column_set - set(files.get(name).columns)

        if missing_columns:
            raise KeyError(
                f"El fichero {name} no contiene las siguientes columnas: {missing_columns}"
            )
