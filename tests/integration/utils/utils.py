from routes.carga.publicacion.datos_carga_publicacion import DatosCargaPublicacion


def estadisticas_datos_publicacion(
    lista_datos_publicacion: list[DatosCargaPublicacion],
):
    plantilla = {
        "titulo": 0,
        "titulo_alternativo": 0,
        "tipo": 0,
        "tipos_publicacion": set(),
        "autores": {
            "existen_autores": 0,
            "existe_firmas": 0,
            "existe_tipos": 0,
            "tipos_autor": set(),
            "existe_orden": 0,
            "existe_contacto": 0,
            "existen_ids": 0,
            "existen_afiliaciones": 0,
            "existe_nombre_afiliacion": 0,
            "existe_pais_afiliacion": 0,
            "existe_ciudad_afiliacion": 0,
            "existe_ror_afiliacion": 0,
        },
        "año_publicacion": 0,
        "identificadores": {
            "doi": 0,
            "idus": 0,
            "scopus": 0,
            "wos": 0,
            "openalex": 0,
        },
        "datos": {
            "volumen": 0,
            "pag_inicio": 0,
            "pag_fin": 0,
            "numero": 0,
            "num_articulo": 0,
        },
        "tipos_fuente": set(),
        "fuente": {
            "titulo": 0,
            "tipo": 0,
            "editoriales": 0,
            "existe_nombre_editorial": 0,
            "existe_tipo_editorial": 0,
            "existe_pais_editorial": 0,
            "existe_url_editorial": 0,
            "identificadores": {
                "issn": 0,
                "eissn": 0,
                "isbn": 0,
                "eisbn": 0,
            },
            "datos": {
                "titulo_alt": 0,
                "url": 0,
            },
        },
        "financiacion": 0,
        "existe_proyecto_financiacion": 0,
        "existe_entidad_financiadora_financiacion": 0,
        "existe_pais_financiacion": 0,
        "existe_ror_financiacion": 0,
        "fechas_publicacion": {
            "publicacion": 0,
            "early_access": 0,
            "insercion": 0,
        },
        "acceso_abierto": 0,
    }

    total = 0

    for datos_publicacion in lista_datos_publicacion:
        total += 1
        if datos_publicacion.titulo:
            plantilla["titulo"] += 1
        if datos_publicacion.titulo_alternativo:
            plantilla["titulo_alternativo"] += 1
        if datos_publicacion.tipo:
            plantilla["tipo"] += 1
            plantilla["tipos_publicacion"].add(datos_publicacion.tipo)
        if datos_publicacion.autores:
            plantilla["autores"]["existen_autores"] += 1
            if any(autor.ids for autor in datos_publicacion.autores):
                plantilla["autores"]["existen_ids"] += 1
            if any(autor.afiliaciones for autor in datos_publicacion.autores):
                plantilla["autores"]["existen_afiliaciones"] += 1
                if any(
                    afiliacion.nombre
                    for autor in datos_publicacion.autores
                    for afiliacion in autor.afiliaciones
                ):
                    plantilla["autores"]["existe_nombre_afiliacion"] += 1
                if any(
                    afiliacion.pais
                    for autor in datos_publicacion.autores
                    for afiliacion in autor.afiliaciones
                ):
                    plantilla["autores"]["existe_pais_afiliacion"] += 1
                if any(
                    afiliacion.ciudad
                    for autor in datos_publicacion.autores
                    for afiliacion in autor.afiliaciones
                ):
                    plantilla["autores"]["existe_ciudad_afiliacion"] += 1
                if any(
                    afiliacion.ror_id
                    for autor in datos_publicacion.autores
                    for afiliacion in autor.afiliaciones
                ):
                    plantilla["autores"]["existe_ror_afiliacion"] += 1

            if any(autor.firma for autor in datos_publicacion.autores):
                plantilla["autores"]["existe_firmas"] += 1
            if any(autor.tipo for autor in datos_publicacion.autores):
                plantilla["autores"]["existe_tipos"] += 1
                plantilla["autores"]["tipos_autor"] = plantilla["autores"][
                    "tipos_autor"
                ].union(set(autor.tipo for autor in datos_publicacion.autores))
            if any(autor.orden for autor in datos_publicacion.autores):
                plantilla["autores"]["existe_orden"] += 1
            if any(autor.contacto != "N" for autor in datos_publicacion.autores):
                plantilla["autores"]["existe_contacto"] += 1

        if datos_publicacion.año_publicacion:
            plantilla["año_publicacion"] += 1

        if datos_publicacion.identificadores:
            for identificador in datos_publicacion.identificadores:
                if identificador.tipo == "doi":
                    plantilla["identificadores"]["doi"] += 1
                if identificador.tipo == "idus":
                    plantilla["identificadores"]["idus"] += 1
                if identificador.tipo == "scopus":
                    plantilla["identificadores"]["scopus"] += 1
                if identificador.tipo == "wos":
                    plantilla["identificadores"]["wos"] += 1
                if identificador.tipo == "openalex":
                    plantilla["identificadores"]["openalex"] += 1

        if datos_publicacion.datos:
            for dato in datos_publicacion.datos:
                if dato.tipo == "volumen":
                    plantilla["datos"]["volumen"] += 1
                if dato.tipo == "pag_inicio":
                    plantilla["datos"]["pag_inicio"] += 1
                if dato.tipo == "pag_fin":
                    plantilla["datos"]["pag_fin"] += 1
                if dato.tipo == "numero":
                    plantilla["datos"]["numero"] += 1
                if dato.tipo == "num_articulo":
                    plantilla["datos"]["num_articulo"] += 1

        if datos_publicacion.fuente:
            if datos_publicacion.fuente.titulo:
                plantilla["fuente"]["titulo"] += 1
            if datos_publicacion.fuente.tipo:
                plantilla["fuente"]["tipo"] += 1
                plantilla["tipos_fuente"].add(datos_publicacion.fuente.tipo)
            if datos_publicacion.fuente.editoriales:
                plantilla["fuente"]["editoriales"] += 1
                if any(
                    editorial.nombre
                    for editorial in datos_publicacion.fuente.editoriales
                ):
                    plantilla["fuente"]["existe_nombre_editorial"] += 1
                if any(
                    editorial.tipo != "Otros"
                    for editorial in datos_publicacion.fuente.editoriales
                ):
                    plantilla["fuente"]["existe_tipo_editorial"] += 1
                if any(
                    editorial.pais != "Desconocido"
                    for editorial in datos_publicacion.fuente.editoriales
                ):
                    plantilla["fuente"]["existe_pais_editorial"] += 1
                if any(
                    editorial.url for editorial in datos_publicacion.fuente.editoriales
                ):
                    plantilla["fuente"]["existe_url_editorial"] += 1
            if datos_publicacion.fuente.identificadores:
                if any(
                    identificador.tipo == "issn"
                    for identificador in datos_publicacion.fuente.identificadores
                ):
                    plantilla["fuente"]["identificadores"]["issn"] += 1
                if any(
                    identificador.tipo == "eissn"
                    for identificador in datos_publicacion.fuente.identificadores
                ):
                    plantilla["fuente"]["identificadores"]["eissn"] += 1
                if any(
                    identificador.tipo == "isbn"
                    for identificador in datos_publicacion.fuente.identificadores
                ):
                    plantilla["fuente"]["identificadores"]["isbn"] += 1
                if any(
                    identificador.tipo == "eisbn"
                    for identificador in datos_publicacion.fuente.identificadores
                ):
                    plantilla["fuente"]["identificadores"]["eisbn"] += 1
            if datos_publicacion.fuente.datos:
                if any(
                    dato.tipo == "titulo_alt" for dato in datos_publicacion.fuente.datos
                ):
                    plantilla["fuente"]["datos"]["titulo_alt"] += 1
                if any(dato.tipo == "url" for dato in datos_publicacion.fuente.datos):
                    plantilla["fuente"]["datos"]["url"] += 1

        if datos_publicacion.financiacion:
            plantilla["financiacion"] += 1
            if any(
                financiacion.proyecto for financiacion in datos_publicacion.financiacion
            ):
                plantilla["existe_proyecto_financiacion"] += 1
            if any(
                financiacion.entidad_financiadora
                for financiacion in datos_publicacion.financiacion
            ):
                plantilla["existe_entidad_financiadora_financiacion"] += 1
            if any(
                financiacion.pais for financiacion in datos_publicacion.financiacion
            ):
                plantilla["existe_pais_financiacion"] += 1
            if any(financiacion.ror for financiacion in datos_publicacion.financiacion):
                plantilla["existe_ror_financiacion"] += 1

        if datos_publicacion.fechas_publicacion:
            for fecha in datos_publicacion.fechas_publicacion:
                if any(
                    fecha.tipo == "publicacion"
                    for fecha in datos_publicacion.fechas_publicacion
                ):
                    plantilla["fechas_publicacion"]["publicacion"] += 1
                if any(
                    fecha.tipo == "early_access"
                    for fecha in datos_publicacion.fechas_publicacion
                ):
                    plantilla["fechas_publicacion"]["early_access"] += 1
                if any(
                    fecha.tipo == "insercion"
                    for fecha in datos_publicacion.fechas_publicacion
                ):
                    plantilla["fechas_publicacion"]["insercion"] += 1

        if datos_publicacion.acceso_abierto:
            plantilla["acceso_abierto"] += 1

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
