from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, PageTemplate, BaseDocTemplate, Frame, Paragraph, Spacer, Table, TableStyle, KeepTogether
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch

from datetime import datetime

import copy


def generar_pdf(resumen, filename):

    # CREA EL HEADER DEL DOCUMENTO CON LOGOS Y TÍTULO
    def header(canvas, doc):
        canvas.saveState()
        total_width = doc.pagesize[0]
        # Imágenes
        canvas.drawImage(
            'routes/informes/pub_metrica/static/logo_us.png', (total_width - 127*0.5)/2 - 220, 700, width=127*0.5, height=112*0.5, mask='auto')
        canvas.drawImage(
            'routes/informes/pub_metrica/static/logo_bib.png', (total_width - 332*0.5)/2 - 20, 695, width=322*0.5, height=115*0.5, mask='auto')
        canvas.drawImage(
            'routes/informes/pub_metrica/static/logo_prisma.png', (total_width - 2046 * 0.08)/2 + 200, 710, width=2046 * 0.08, height=380 * 0.08, mask='auto')

        # Título del header
        tipo_fuente = list(resumen["titulo"].keys())[0]
        nombre_fuente = resumen["titulo"][tipo_fuente]
        titulo = f"Informe de {tipo_fuente} ({resumen['año_inicio']}-{resumen['año_fin']}): {nombre_fuente}"
        fecha_actual = datetime.now().strftime("%d/%m/%Y %H:%M")
        fecha_emision = f"Fecha de emisión: {fecha_actual}"

        # Ajustes de centrado, fuente y posición
        canvas.setFont("Helvetica", 12)
        text_width = canvas.stringWidth(titulo, 'Helvetica', 12)
        x_centered = (doc.pagesize[0] - text_width) / 2
        canvas.drawString(x_centered, 650, titulo)

        text_width = canvas.stringWidth(fecha_emision, 'Helvetica', 12)
        x_centered = (doc.pagesize[0] - text_width) / 2
        canvas.drawString(x_centered, 625, fecha_emision)

        canvas.restoreState()

    # Plantilla base del documento
    document = BaseDocTemplate(filename, pagesize=letter)

    # Elemento separador para introducir entre párrafos/tablas
    whitespace = Spacer(1, 0.25 * inch)

    # Márgenes del documento
    frame = Frame(document.leftMargin, document.bottomMargin,
                  document.width, document.height - 105)

    # Plantilla de la página que contiene el header del documento
    template = PageTemplate(id='header', frames=[frame], onPage=header)
    document.addPageTemplates(template)

    # Elementos del documento
    elements = []

    # Estilos de parrafos
    estilo_titulos = ParagraphStyle(
        name="EstiloTitulos",
        fontName="Helvetica-Bold",
        fontSize=16,
        alignment=1,  # 0=left, 1=center, 2=right
    )

    estilo_subtitulos = ParagraphStyle(
        name="EstiloSubtitulos",
        fontName="Helvetica-Bold",
        fontSize=12,
        alignment=0,  # 0=left, 1=center, 2=right
    )
    # Estilo base de tablas
    table_style = TableStyle([
        # Fondo de la primera columna
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#9c1f2f")),
        # # Color de texto de la primera columna
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        # Centrar elementos de la primera columna
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        # Primera columna en negrita
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        # Centrar primera columna
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -2), 1, colors.black)
    ])

    table_style_normal = TableStyle(table_style.getCommands())
    table_style_normal.add('GRID', (0, 0), (-1, -1), 1, colors.black)

    propiedades_leyenda = [
        ("SPAN", (0, -1), (-1, -1)),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, -1), (-1, -1), 8),
        ("TEXTCOLOR", (0, -1), (-1, -1), colors.grey),
        ("BACKGROUND", (0, -1), (-1, -1), colors.white),
        ("BOX", (0, -1), (-1, -1), 1, colors.white),
        ('GRID', (0, 1), (-1, -2), 1, colors.black),
        ("ALIGN", (0, -1), (-1, -1), "LEFT"),
    ]
    table_style_leyenda = TableStyle(table_style.getCommands())
    for propiedad in propiedades_leyenda:
        table_style_leyenda.add(*propiedad)

    # Estilo de tablas matriz
    table_style_matriz = TableStyle([
        ('BACKGROUND', (1, 0), (-1, 0), colors.HexColor("#9c1f2f")),
        ('BACKGROUND', (0, 1), (0, -1), colors.HexColor("#9c1f2f")),
        ('TEXTCOLOR', (1, 0), (-1, 0), colors.whitesmoke),
        ('TEXTCOLOR', (0, 1), (0, -1), colors.whitesmoke),
        ('ALIGN', (1, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (0, 1), (0, -1), 'CENTER'),
        ('FONTNAME', (1, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ('VALIGN', (1, 0), (-1, 0), 'MIDDLE'),
        ('VALIGN', (0, 1), (0, -1), 'MIDDLE'),
        ('GRID', (1, 0), (-1, -2), 1, colors.black),
        ('GRID', (0, 1), (-1, -2), 1, colors.black),
    ])
    table_style_matriz_normal = TableStyle(table_style_matriz.getCommands())
    table_style_matriz_normal.add('GRID', (1, 0), (-1, -1), 1, colors.black),
    table_style_matriz_normal.add('GRID', (0, 1), (-1, -1), 1, colors.black),

    table_style_matriz_leyenda = TableStyle(
        table_style_matriz.getCommands())
    for propiedad in propiedades_leyenda:
        table_style_matriz_leyenda.add(*propiedad)
    # MIEMBROS

    titulo_miembros = Paragraph("Datos de miembros", estilo_titulos)

    # TABLA DE RAMAS
    datos_tabla_ramas = [['Rama US', 'Área de Conocimiento', 'Miembros']]

    for key, value in resumen["investigadores_por_rama"].items():
        row = [value['Rama US'],
               value['Área de Conocimiento'], value['Miembros']]
        datos_tabla_ramas.append(row)

    # Leyenda
    datos_tabla_ramas.append(
        [f"Número de miembros: {resumen['cantidad_investigadores']}", '', ''])

    tabla_ramas = Table(datos_tabla_ramas)
    tabla_ramas.setStyle(table_style_leyenda)

    # TABLA DE CATERGORÍAS PROFESIONALES
    datos_tabla_categorias_profesionales = [
        ['Categoría Profesional', 'Miembros']]

    for key, value in resumen["investigadores_por_categoria"].items():
        row = [value['Categoría Profesional'], value['Miembros']]
        datos_tabla_categorias_profesionales.append(row)

    # Leyenda
    datos_tabla_categorias_profesionales.append(
        [f"Número de miembros: {resumen['cantidad_investigadores']}", ''])

    tabla_categorias_profesionales = Table(
        datos_tabla_categorias_profesionales)
    tabla_categorias_profesionales.setStyle(table_style_leyenda)

    elements.append(KeepTogether([
        KeepTogether(titulo_miembros),
        whitespace,
        KeepTogether(tabla_ramas),
        whitespace,
        KeepTogether(tabla_categorias_profesionales),
        whitespace]
    ))

    # DATOS DE PUBLICACIONES

    titulo_publicaciones = Paragraph("Datos de publicaciones", estilo_titulos)

    # TABLA DE PUBLICACIONES POR AÑO
    publicaciones_por_año = resumen["publicaciones_por_año"]
    datos_tabla_publicaciones_por_año = [
        [''], ['Nº de publicaciones']
    ]

    total_publicaciones = 0
    for key, value in publicaciones_por_año.items():
        cantidad_publicaciones = value['Nº de publicaciones']
        datos_tabla_publicaciones_por_año[0].append(key)
        datos_tabla_publicaciones_por_año[1].append(
            cantidad_publicaciones)
        total_publicaciones += cantidad_publicaciones

    datos_tabla_publicaciones_por_año[0].append("Total")
    datos_tabla_publicaciones_por_año[1].append(total_publicaciones)

    tabla_publicaciones_por_año = Table(
        datos_tabla_publicaciones_por_año, colWidths=100)
    tabla_publicaciones_por_año.setStyle(table_style_matriz_normal)

    # TABLA DE TIPOS DE PUBLICACIONES
    publicaciones_por_tipo = resumen["publicaciones_por_tipo"]
    datos_tabla_publicaciones_por_tipo = [
        ['Tipo', 'Nº de publicaciones']]

    for key, value in publicaciones_por_tipo.items():
        datos_tabla_publicaciones_por_tipo.append(
            [value["Tipo"], value["Nº de publicaciones"]])

    tabla_publicaciones_por_tipo = Table(
        datos_tabla_publicaciones_por_tipo, colWidths=150)
    tabla_publicaciones_por_tipo.setStyle(table_style_normal)

    # TABLA DE PUBLICACIONES POR AUTORIA
    publicaciones_por_autoria = resumen["publicaciones_por_autoria"]
    datos_tabla_publicaciones_por_autoria = [["", "Nº de publicaciones"]]

    for key, value in publicaciones_por_autoria.items():
        datos_tabla_publicaciones_por_autoria.append([key, value])

    tabla_publicaciones_por_autoria = Table(
        datos_tabla_publicaciones_por_autoria, colWidths=150
    )
    tabla_publicaciones_por_autoria.setStyle(table_style_matriz_normal)

    # TABLAS DE CITAS
    citas_publicaciones = resumen["citas_publicaciones"]
    columnas = ['', 'Base de datos', 'Publicaciones',
                'Citas', 'Media de citas', 'Índice H']

    # Crear tablas separadas para publicaciones y publicaciones grupales
    datos_tabla_citas_publicaciones = [copy.deepcopy(columnas)]
    datos_tabla_citas_publicaciones_grupales = [copy.deepcopy(columnas)]

    map_base_datos = {
        "wos": "WoS",
        "scopus": "Scopus",
    }
    for key, value in citas_publicaciones.items():
        año = key
        # Cada entrada de año tiene los datos separados por base de datos, los evaluamos por separado
        for base_datos, datos in value.items():
            # Separamos datos de pulicaciones y publicaciones grupales, y almacenamos la fila
            datos_publicaciones = datos["publicaciones"]
            datos_publicaciones_grupales = datos["publicaciones_autoria_grupal"]
            datos_tabla_citas_publicaciones.append(
                [año, map_base_datos[base_datos],
                 datos_publicaciones["cantidad_publicaciones"],
                 datos_publicaciones["citas"],
                 datos_publicaciones["media_citas"],
                 datos_publicaciones["indice_h"]])
            datos_tabla_citas_publicaciones_grupales.append(
                [año, map_base_datos[base_datos],
                 datos_publicaciones_grupales["cantidad_publicaciones"],
                 datos_publicaciones_grupales["citas"],
                 datos_publicaciones_grupales["media_citas"],
                 datos_publicaciones_grupales["indice_h"]])

    # Modificar el estilo de la tabla para fusionar cada dos celdas en la primera columna
    table_styles_citas_publicaciones = TableStyle(
        table_style_matriz_leyenda.getCommands())
    for row in range(1, len(datos_tabla_citas_publicaciones) - 1, 2):
        table_styles_citas_publicaciones.add("SPAN", (0, row), (0, row + 1))

    datos_tabla_citas_publicaciones.append(
        ["En esta tabla se excluyen las publicaciones grupales"])
    tabla_citas_publicaciones = Table(
        datos_tabla_citas_publicaciones, colWidths=75
    )
    tabla_citas_publicaciones.setStyle(table_styles_citas_publicaciones)

    datos_tabla_citas_publicaciones_grupales.append(
        ["Publicaciones cuya autoría es exclusivamente grupal para el conjunto de investigadores del informe"])
    tabla_citas_publicaciones_grupales = Table(
        datos_tabla_citas_publicaciones_grupales, colWidths=75
    )
    tabla_citas_publicaciones_grupales.setStyle(
        table_styles_citas_publicaciones)

    elements.append(KeepTogether(
        [titulo_publicaciones,
         whitespace,
         KeepTogether(tabla_publicaciones_por_año),
         whitespace,
         KeepTogether(tabla_publicaciones_por_tipo),
         whitespace,
         KeepTogether(tabla_publicaciones_por_autoria),
         whitespace,
         KeepTogether([KeepTogether([Paragraph("Citas de publicaciones:", estilo_subtitulos),
                       whitespace,
                      tabla_citas_publicaciones]),
                       whitespace,
                       KeepTogether([Paragraph("Citas de publicaciones grupales:", estilo_subtitulos),
                                     whitespace,
                                     tabla_citas_publicaciones_grupales]),]),
         whitespace,]
    ))
    # TABLAS DE FACTORES DE IMPACTO
    titulo_factores_impacto = Paragraph(
        "Distribución de publicaciones", estilo_titulos)

    distribucion_publicaciones = resumen["distribucion_publicaciones"]

    datos_tabla_cuartiles = [["", "Q1", "Q2",
                              "Q3", "Q4", "Total", "No incluidas"]]

    datos_tabla_deciles_d1_d5 = [["", "D1", "D2", "D3", "D4", "D5"]]
    datos_tabla_deciles_d5_d10 = [["", "D6", "D7", "D8", "D9", "D10"]]
    datos_tabla_deciles_total = [["", "Total", "No incluidas"]]

    for key, value in distribucion_publicaciones.items():
        basedatos = key
        cuartiles = value["cuartiles"]
        fila_cuartiles = []
        fila_cuartiles.append(basedatos)
        for cuartil, datos in cuartiles["incluidas"].items():
            if isinstance(datos, dict):
                fila_cuartiles.append(
                    f"{datos['valor']}({datos['porcentaje']})")
            else:
                fila_cuartiles.append(datos)
        fila_cuartiles.append(cuartiles["no_incluidas"])
        datos_tabla_cuartiles.append(fila_cuartiles)

        deciles = value["deciles"]
        fila_deciles = []
        fila_deciles.append(basedatos)
        for decil, datos in deciles["incluidas"].items():
            if isinstance(datos, dict):
                fila_deciles.append(
                    f"{datos['valor']}({datos['porcentaje']})")
            else:
                fila_deciles.append(datos)
        fila_deciles.append(deciles["no_incluidas"])

        # Crear las distintas tablas de deciles
        datos_tabla_deciles_d1_d5.append(fila_deciles[0:6])
        datos_tabla_deciles_d5_d10.append(
            [fila_deciles[0]] + fila_deciles[6:11])
        datos_tabla_deciles_total.append(
            [fila_deciles[0]] + fila_deciles[11:13])

    # Leyenda
    leyenda_distribucion_publicaciones = "JIF: Journal Impact Factor; SJR: Scimago Journal & Country Rank"
    datos_tabla_cuartiles.append(
        [leyenda_distribucion_publicaciones])
    datos_tabla_deciles_total.append(
        [leyenda_distribucion_publicaciones])

    tabla_cuartiles = Table(datos_tabla_cuartiles, colWidths=70)
    tabla_cuartiles.setStyle(table_style_matriz_leyenda)

    tabla_deciles_d1_d5 = Table(datos_tabla_deciles_d1_d5, colWidths=70)
    tabla_deciles_d1_d5.setStyle(table_style_matriz_normal)
    tabla_deciles_d5_d10 = Table(datos_tabla_deciles_d5_d10, colWidths=70)
    tabla_deciles_d5_d10.setStyle(table_style_matriz_normal)
    tabla_deciles_total = Table(
        datos_tabla_deciles_total, colWidths=[70, 175, 175])
    tabla_deciles_total.setStyle(table_style_matriz_leyenda)

    elements.append(KeepTogether
                    ([KeepTogether(titulo_factores_impacto),
                     whitespace,
                     KeepTogether(tabla_cuartiles),
                     whitespace,
                     KeepTogether(
                         [tabla_deciles_d1_d5, tabla_deciles_d5_d10, tabla_deciles_total]),
                     whitespace,
                      ]))

    document.build(elements)
