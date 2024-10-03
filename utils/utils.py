# Dada una lista, devuelve un diccionario que mapea cada elemento de la lista con su índice correspondiente
def list_index_map(input):
    return {element: index for index, element in enumerate(input)}


# Dada una lista de listas (como el resultado de una consulta), devuelve lo mismo con los valores nulos sustituidos


def replace_none_values(input_list, replacement_char="-"):
    return [
        [replacement_char if item is None else item for item in sublist]
        for sublist in input_list
    ]


def constrain_to_range(value, min_value, max_value):
    return max(min(value, max_value), min_value)
