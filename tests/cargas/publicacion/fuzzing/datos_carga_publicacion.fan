<start> ::= <json>
<json> ::= "{"
        '"titulo": ' <titulo> ', '
        '"titulo_alternativo": ' <titulo_alternativo> ', '
        '"tipo": ' <tipo_publicacion> ', '
        '"autores": ' <autores> ', '
        '"año_publicacion": ' <año_publicacion> ', '
        '"fechas_publicacion": ' <fechas_publicacion> ', '
        '"identificadores": ' <identificadores_publicacion> ', '
        '"datos": ' <datos_publicacion> ', '
        '"financiacion": ' <financiacion> ', '
        '"acceso_abierto": '  <acceso_abierto> ', '
        '"fuente": ' <fuente>
        "}"


#GRAMÁTICA DE ATRIBUTOS DE PUBLICACIÓN
<titulo> ::= r'"[a-zA-Z_][a-zA-Z0-9_]{1,1000}"'
<titulo_alternativo> ::= r'"[a-zA-Z_][a-zA-Z0-9_]{1,300}"' | '""'
<tipo_publicacion> ::= '"Artículo"' | '"Book Chapter"' | '"Capítulo"' | '"Clinical Study"' | '"Clinical Trial Protocol"' | '"Clinical Trial"' | '"Comment"' | '"Comparative Study"' | '"Conference Review"' | '"Consensus Development Conference"' | '"Corrección"' | '"Data Paper"' | '"Dataset"' | '"Desconocido"' | '"Divulgación"' | '"edited-book"' | '"Editorial"' | '"English Abstract"' | '"Letter"' | '"Libro"' | '"Meeting"' | '"monograph"' | '"Nota"' | '"Otros"' | '"Otros1"' | '"Otros4"' | '"Poetry"' | '"Ponencia"' | '"posted-content"' | '"Póster"' | '"Practice Guideline"' | '"proceedings"' | '"Published Erratum"' | '"reference-book"' | '"report"' | '"Reprint"' | '"Reseña"' | '"Resumen congreso"' | '"Retraction of Publication"' | '"review-article"' | '"Revisión"' | '"Short Survey"' | '"Systematic Review"'


#GRAMÁTICA DE AUTOR
<autores> ::= '[' <autor> (", " <autor>){0,99} ']'
<autor> ::=         '{ '
                    '"firma": '  <firma_autor> ', '
                    '"tipo": '  <tipo_autor> ', '
                    '"orden": '  <orden_autor> ', '
                    '"contacto": '  <contacto_autor> ', '
                    '"ids": ' <ids_autor> ', '
                    '"afiliaciones": ' <afiliaciones_autor>
                    '}'

<firma_autor> ::= r'"[a-zA-Z_][a-zA-Z0-9_]{1,10}"'
<tipo_autor> ::= '"Autor/a"'
<orden_autor> ::= <number>
<contacto_autor> ::= '"S"' | '"N"'

<ids_autor> ::= '[' <id_autor> ']' 
<id_autor> ::= '{'
                '"tipo": ' <tipo_id_autor> ', '
                '"valor": ' <valor_id_autor>
                '}'

<tipo_id_autor> ::= '"idus"'
<valor_id_autor> ::= r'"[0-9]{10}"'


#GRAMÁTICA DE AFILICACIONES DE AUTOR
<afiliaciones_autor> ::= '[' <afiliacion_autor> (', ' <afiliacion_autor> ){0,30}']'
<afiliacion_autor> ::= '{'
                        '"nombre": ' <nombre_afiliacion_autor> ', '
                        '"pais": ' <pais_afiliacion_autor> ', '
                        '"ciudad": ' <ciudad_afiliacion_autor> ', '
                        '"ror_id": ' <ror>
                        '}'

<nombre_afiliacion_autor> ::= r'"[a-zA-Z_][a-zA-Z0-9_]{1,100}"'
<pais_afiliacion_autor> ::= r'"[a-zA-Z_][a-zA-Z0-9_]{1,100}"'
<ciudad_afiliacion_autor> ::= r'"[a-zA-Z_][a-zA-Z0-9_]{1,100}"'


# GRAMÁTICA FECHA DE PUBLICACIÓN
<año_publicacion> ::= '"'<year>'"'

<fechas_publicacion> ::= '[' 
                        <fecha_publicacion> 
                        (',' <fecha_insercion>)?
                        (',' <fecha_early_access>)?
                        ']'

<fecha_publicacion_base> ::= '{' 
                            '"dia": ' <dia_fecha_publicacion> ', '
                            '"mes": ' <mes_fecha_publicacion> ', '
                            '"agno": ' <año_fecha_publicacion> ', '
                            '"tipo": ' <tipo_fecha_publicacion>
                            '}'


<fecha_publicacion> ::= <fecha_publicacion_base>
<fecha_insercion> ::= <fecha_publicacion_base>
<fecha_early_access> ::= <fecha_publicacion_base>

<dia_fecha_publicacion> ::= <day> | 'null'
<mes_fecha_publicacion> ::= <month> | 'null'
<año_fecha_publicacion> ::= <year>
<tipo_fecha_publicacion> ::= '"publicacion"' | '"early_access"' | '"insercion"'

# GRAMÁTICA DE IDENTIFICADORES DE PUBLICACIÓN
<identificadores_publicacion> ::= '['
                                <identificador_doi>
                                (',' <identificador_idus>)?
                                ']'

<identificador_doi> ::= '{'
    '"tipo": "doi", ' 
    '"valor": ' '"'<doi>'"'
    '}'

<identificador_idus> ::= '{'
    '"tipo": "idus", ' 
    '"valor": ' '"'<idus>'"'
    '}'

<doi> ::= r'10\.\d{4,9}/[-._;()/:A-Z0-9]+'
<idus> ::= r'11441/[0-9]{5-6}'

# GRAMÁTICA DE DATOS DE PUBLICACIÓN
<datos_publicacion> ::= '['
                        <dato_publicacion_volumen> ', ' <dato_publicacion_pag_inicio> ', ' <dato_publicacion_pag_fin>
                        ']'

<dato_publicacion_volumen> ::= '{' 
                        '"tipo": "volumen"'  ', '
                        '"valor": ' <valor_dato_publicacion>
                        '}'

<dato_publicacion_pag_inicio> ::= '{' 
                        '"tipo": "pag_inicio"'  ', '
                        '"valor": ' <valor_dato_publicacion>
                        '}'

<dato_publicacion_pag_fin> ::= '{' 
                        '"tipo": "pag_fin"'  ', '
                        '"valor": ' <valor_dato_publicacion>
                        '}'

<valor_dato_publicacion> ::= r'"[1-9][0-9]{0,2}"'


# GRAMÁTICA DE FINANCIACIÓN
<financiacion> ::= '['
                    <proyecto> (', ' <proyecto>){0,9}
                    ']'

<proyecto> ::= '{'
                '"proyecto": ' <referencia_proyecto> ', '
                '"entidad_financiadora": ' <entidad_financiadora_proyecto> ', '
                '"agencia": ' <agencia_proyecto> ', '
                '"pais": ' <pais_proyecto> ', '
                '"ror": ' <ror>
                '}'

<referencia_proyecto> ::= r'"[A-Z0-9]{4,5}-[A-Z0-9]{4,7}(-[A-Z0-9]{2-3}){0,2}"'
<entidad_financiadora_proyecto> ::= r'"[a-zA-Z ]{1,100}"'
<agencia_proyecto> ::= r'"[a-zA-Z ]{1,50}"'
<pais_proyecto> ::= r'"[a-zA-Z ]{1,50}"'


# GRAMÁTICA DE OPEN ACCESS
<acceso_abierto> ::= '['
                    <openaccess> (',' <openaccess>){0,2}
                    ']'
<openaccess> ::= '{'
                '"origen": ' <tipo_openaccess> ', '
                '"valor": ' <valor_openaccess>
                '}'

<tipo_openaccess> ::= '"upw"'
<valor_openaccess> ::= '"green"' | '"gold"' | '"hybrid"' | '"bronze"' | '"closed"'



# GRAMÁTICA DE FUENTE
<fuente> ::= '{'
            '"titulo": ' <titulo_fuente> ', '
            '"tipo": ' <tipo_fuente> ', '
            '"editoriales": ' <editoriales_fuente> ', '
            '"identificadores": ' <identificadores_fuente> ', '
            '"datos": ' <datos_fuente>
            '}'


# GRAMÁTICA DE ATRIBUTOS DE FUENTE
<titulo_fuente> ::= r'"[a-zA-Z]{1,100}"'
<tipo_fuente> ::= '"Book in series"' | '"Book Series"' | '"Books in series"' | '"Books"' | '"Colección"' | '"Conference Proceeding"' | '"Congreso"' | '"Desconocido"' | '"Libro"' | '"Monográfico"' | '"Prensa"' | '"Repositorio"' | '"Revista"' | '"Trade Journal"'

# GRAMÁTICA DE EDITORIALES DE FUENTE

<editoriales_fuente> ::= '[' <editorial_fuente> (', ' <editorial_fuente>){0,2} ']'
<editorial_fuente> ::= '{'
                '"nombre": ' <nombre_editorial_fuente> ', '
                '"tipo": ' <tipo_editorial_fuente> ', '
                '"pais": ' <pais_editorial_fuente> ', '
                '"url": ' <url_editorial_fuente> 
                '}'

<nombre_editorial_fuente> ::= r'"[a-zA-Z ]{1,50}"'
<tipo_editorial_fuente> ::= '"administracion"' | '"comercial"' | '"cormercial"' | '"Otros"' | '"universidad"'
<pais_editorial_fuente> ::= r'"[a-zA-Z ]{1,50}"'
<url_editorial_fuente> ::= <url>

# GRAMÁTICA DE IDENTIFICADORES DE FUENTE
<identificadores_fuente> ::= '['
                            <identificador_fuente> (', ' <identificador_fuente>){0,3}
                            ']'
<identificador_fuente> ::= <identificador_fuente_issn> | <identificador_fuente_eissn> | <identificador_fuente_isbn> | <identificador_fuente_eisbn> 

<identificador_fuente_issn> ::= '{'
            '"tipo": "issn"'  ', '
            '"valor": ' <issn>
            '}'

<identificador_fuente_eissn> ::= '{'
            '"tipo": "eissn"'  ', '
            '"valor": ' <issn>
            '}'

<identificador_fuente_isbn> ::= '{'
            '"tipo": "isbn"'  ', '
            '"valor": ' <isbn>
            '}'

<identificador_fuente_eisbn> ::= '{'
            '"tipo": "eisbn"'  ', '
            '"valor": ' <isbn>
            '}'

# GRAMÁTICA DE DATOS DE FUENTE
<datos_fuente> ::= '[' <dato_fuente_titulo_alt> ', ' <dato_fuente_url> ']'

<dato_fuente_titulo_alt> ::= '{'
                        '"tipo": "url"' ', '
                        '"valor": ' r'"[a-zA-Z ]{1,100}"'
                        '}'
<dato_fuente_url> ::= '{'
                        '"tipo": "url"' ', '
                        '"valor": ' <url>
                        '}'

<year> ::= r'19[7-9][0-9]|20[0-2][0-9]'
<month> ::= r'[0-9]|[1][1-2]'
<day> ::= r'[1-9]|[1-2][1-9]|[3][0-1]'

<issn> ::= r'"[0-9]{4}-[0-9]{3}[0-9X]{1}"'
<isbn> ::= r'"[0-9]{10}"' | r'"[0-9]{13}"' 


<url> ::= r'"https://[a-z]{1,20}\.com"'

<key_value_pairs> ::= <key_value_pair> (',' <key_value_pair>)* 
<key_value_pair> ::= <key> ':' <value> 
<key> ::= r'"[a-zA-Z_][a-zA-Z0-9_]*"' 
<value> ::= <string> | <number> | <object> | <array> | 'true' | 'false' | 'null' 
<string> ::= r'"[^"\\]*(?:\\.[^"\\]*)*"' 
<number> ::= r'-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?' 
<object> ::= '{' <key_value_pairs>? '}' 
<array> ::= '[' <value> (',' <value>)* ']' 

<ror> ::= r'"0[a-hj-km-np-tv-z|0-9]{6}[0-9]{2}"'

def counter(n):
    while True:
        yield n
        n += 1

lista_autores = counter(1)

def siguiente_autor():
    return next(lista_autores)

<orden_autor> ::= <number> := str(siguiente_autor())


where str(<fecha_publicacion>.<fecha_publicacion_base>.<tipo_fecha_publicacion>) == '"publicacion"'
where str(<fecha_early_access>.<fecha_publicacion_base>.<tipo_fecha_publicacion>) == '"early_access"'
where str(<fecha_insercion>.<fecha_publicacion_base>.<tipo_fecha_publicacion>) == '"insercion"'