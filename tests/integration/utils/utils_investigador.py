import re
from routes.carga.investigador.datos_carga_investigador import (
    DatosCargaInvestigador,
    DatosCargaContratoInvestigador,
    DatosCargaCeseInvestigador,
)


def estadisticas_datos_publicacion(
    lista_datos_investigador: list[DatosCargaInvestigador],
):
    plantilla = {
        "nombre": 0,
        "apellidos": 0,
        "documento_identidad": 0,
        "email": 0,
        "nacionalidad": 0,
        "sexo": 0,
        "fecha_nacimiento": 0,
        "contratos": {
            "existen_contratos": 0,
            "fecha_contratacion": 0,
            "fecha_fin_contratacion": 0,
            "fecha_nombramiento": 0,
            "existen_categorias": 0,
            "existen_areas": 0,
            "existen_centro": 0,
            "existen_cese": 0,
            "existen_departamentos": 0,
            "area": {
                "id": 0,
                "nombre": 0,
            },
            "departamento": {
                "id": 0,
                "nombre": 0,
            },
            "centro": {
                "id": 0,
                "nombre": 0,
            },
            "categoria": {
                "id": 0,
                "nombre": 0,
                "femenino": 0,
                "tipo_pp": 0,
            },
            "cese": {
                "fuentes_datos": 0,
                "documento_identidad": 0,
                "tipo": 0,
                "valor": 0,
                "fecha": 0,
                "cese_no_inves": 0,
            },
        },
    }

    total = 0

    for datos_investigador in lista_datos_investigador:
        total += 1
        if datos_investigador.nombre and datos_investigador.nombre != "":
            plantilla["nombre"] += 1
        if datos_investigador.apellidos and datos_investigador.apellidos != "":
            plantilla["apellidos"] += 1
        if (
            datos_investigador.documento_identidad
            and datos_investigador.documento_identidad != ""
        ):
            plantilla["documento_identidad"] += 1
        if datos_investigador.email and datos_investigador.email != "":
            plantilla["email"] += 1
        if datos_investigador.nacionalidad and datos_investigador.nacionalidad != "":
            plantilla["nacionalidad"] += 1
        if datos_investigador.sexo and datos_investigador.sexo != "":
            plantilla["sexo"] += 1
        if (
            datos_investigador.fecha_nacimiento
            and datos_investigador.fecha_nacimiento != ""
        ):
            plantilla["fecha_nacimiento"] += 1
        # CONTRATO
        if datos_investigador.contratos:
            plantilla["contratos"]["existen_contratos"] += 1
        if any(
            contrato.fecha_contratacion and contrato.fecha_contratacion != ""
            for contrato in datos_investigador.contratos
        ):
            plantilla["contratos"]["fecha_contratacion"] += 1
        if any(
            contrato.fecha_fin_contratacion and contrato.fecha_fin_contratacion != ""
            for contrato in datos_investigador.contratos
        ):
            plantilla["contratos"]["fecha_fin_contratacion"] += 1
        if any(
            contrato.fecha_nombramiento and contrato.fecha_nombramiento != ""
            for contrato in datos_investigador.contratos
        ):
            plantilla["contratos"]["fecha_nombramiento"] += 1
        # CONTRATO - CATEGORÍAS
        if any(contrato.categoria for contrato in datos_investigador.contratos):
            plantilla["contratos"]["existen_categorias"] += 1
            if any(
                contrato.categoria.id and contrato.categoria.id != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["categoria"]["id"] += 1
            if any(
                contrato.categoria.nombre and contrato.categoria.nombre != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["categoria"]["nombre"] += 1
            if any(
                contrato.categoria.femenino and contrato.categoria.femenino != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["categoria"]["femenino"] += 1
            if any(
                contrato.categoria.tipo_pp and contrato.categoria.tipo_pp != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["categoria"]["tipo_pp"] += 1
        # CONTRATO - DEPARTAMENTOS
        if any(contrato.area for contrato in datos_investigador.contratos):
            plantilla["contratos"]["existen_departamentos"] += 1
            if any(
                contrato.departamento.id and contrato.departamento.id != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["departamento"]["id"] += 1
            if any(
                contrato.departamento.nombre and contrato.departamento.nombre != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["departamento"]["nombre"] += 1
        # CONTRATO - ÁREAS
        if any(contrato.area for contrato in datos_investigador.contratos):
            plantilla["contratos"]["existen_areas"] += 1
            if any(
                contrato.area.id and contrato.area.id != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["area"]["id"] += 1
            if any(
                contrato.area.nombre and contrato.area.nombre != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["area"]["nombre"] += 1
        # CONTRATO - CENTRO
        if any(contrato.centro for contrato in datos_investigador.contratos):
            plantilla["contratos"]["existen_centro"] += 1
            if any(
                contrato.centro.id and contrato.centro.id != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["centro"]["id"] += 1
            if any(
                contrato.centro.nombre and contrato.centro.nombre != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["centro"]["nombre"] += 1
        # CONTRATO - CESE
        if any(contrato.cese for contrato in datos_investigador.contratos):
            plantilla["contratos"]["existen_cese"] += 1
            if any(
                contrato.cese.fuente_datos and contrato.cese.fuente_datos != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["cese"]["fuentes_datos"] += 1
            if any(
                contrato.cese.documento_identidad
                and contrato.cese.documento_identidad != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["cese"]["documento_identidad"] += 1
            if any(
                contrato.cese.tipo and contrato.cese.tipo != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["cese"]["tipo"] += 1
            if any(
                contrato.cese.valor and contrato.cese.valor != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["cese"]["valor"] += 1
            if any(
                contrato.cese.fecha and contrato.cese.fecha != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["cese"]["fecha"] += 1
            # cese que no tiene contrato ni investigador asociado
            if any(
                contrato.fecha_contratacion == ""
                and contrato.cese.fecha != ""
                and contrato.cese.documento_identidad != ""
                for contrato in datos_investigador.contratos
            ):
                plantilla["contratos"]["cese"]["cese_no_inves"] += 1

    for key, value in plantilla.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict):
                    for sub_sub_key, sub_sub_value in sub_value.items():
                        if isinstance(sub_sub_value, (int, float)):
                            plantilla[key][sub_key][sub_sub_key] = (
                                round(((sub_sub_value / total) * 100), 2)
                                if total > 0
                                else 0
                            )
                elif isinstance(sub_value, (int, float)):
                    plantilla[key][sub_key] = (
                        round(((sub_value / total) * 100), 2) if total > 0 else 0
                    )
        elif isinstance(value, (int, float)):
            plantilla[key] = round(((value / total) * 100), 2) if total > 0 else 0

    return plantilla
