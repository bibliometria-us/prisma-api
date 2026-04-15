# Errores globales
responses = {
    200: "La solicitud ha tenido éxito",
    401: "No autorizado",
    406: "Formato de salida no soportado",
    500: "Error del servidor",
}
params = {
    "api_key": {
        "name": "api_key",
        "description": "API Key",
        "type": "string",
    },
    "salida": {
        "name": "salida",
        "description": "Formato de salida. Especificar en este campo en caso de que no pueda hacerlo mediante el header de la petición",
        "type": "string",
        "enum": ["json", "xml", "csv"],
    },
}

paginate_params = {
    "pagina": {
        "name": "pagina",
        "description": "Número de página",
        "type": "int",
    },
    "longitud_pagina": {
        "name": "Longitud de páginas",
        "description": "Cantidad de elementos por página",
        "type": "int",
    },
}
