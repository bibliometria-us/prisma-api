# Dada una lista de IDs proveniente de una query, devuelve una lista de esas IDs como str


def normalize_id_list(id_list):
    result = []

    for id in id_list[1:]:
        if isinstance(id, tuple):
            element = id[0]

            if isinstance(element, str):
                result.append(f'"{str(element)}"')
            else:
                result.append(str(element))
    return result


# Dada una lista de sentencias de query, las devuelve aplicado un .format con los pares clave valor del diccionario


def format_query(strings, format_dict):
    formatted_strings = []
    for string in strings:
        try:
            formatted_values = {}

            for key, value in format_dict.items():
                if isinstance(value, list):
                    formatted_values[key] = ", ".join(map(str, value))
                else:
                    formatted_values[key] = value

            formatted_string = string.format(**formatted_values)
            formatted_strings.append(formatted_string)
        except KeyError:
            formatted_strings.append(string)
    return formatted_strings


def calcular_autoria_preferente(investigadores, autores: str):
    candidatos = set()
    if autores == "-" or not autores:
        return "No"
    # Calcular el orden máximo
    autores = autores.strip(";")
    max_orden = max(int(item.split(",")[1]) for item in autores.split(";"))

    # Buscar autores preferentes
    for autor in autores.split(";"):
        atributos = autor.split(",")
        id = atributos[0]
        orden = int(atributos[1])
        correspondencia = atributos[2]
        if id != 0:
            if orden == max_orden:
                candidatos.add(id)

            if correspondencia == "S":
                candidatos.add(id)

            if orden == 1:
                candidatos.add(id)

    # Comprobar si al menos uno de los autores preferentes pertenece al conjunto de investigadores
    if len(set(investigadores).intersection(candidatos)) > 0:
        return "Sí"
    else:
        return "No"
