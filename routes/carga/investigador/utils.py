import re


def validar_dni_nie(dni: str) -> bool:
    """
    Valida el DNI de un investigador.
    l: True si el DNI es válido, False en caso contrario."
    """
    letras = "TRWAGMYFPDXBNJZSQVHLCKE"
    numero = numero.upper().strip().replace("-", "").replace(" ", "")

    # Validación de NIE: empieza por X, Y o Z
    if re.match(r"^[XYZ]\d{7}[A-Z]$", numero):
        # Reemplazar la letra inicial por su equivalente numérico
        conversion = {"X": "0", "Y": "1", "Z": "2"}
        numero_numerico = conversion[numero[0]] + numero[1:-1]
        letra_correcta = letras[int(numero_numerico) % 23]
        return numero[-1] == letra_correcta

    # Validación de DNI: 8 números + 1 letra
    elif re.match(r"^\d{8}[A-Z]$", numero):
        numero_numerico = numero[:-1]
        letra_correcta = letras[int(numero_numerico) % 23]
        return numero[-1] == letra_correcta

    return False  # No coincide con formato de DNI ni NIE


def validar_email(email: str, dominio: str = "us.es") -> bool:
    # Expresión regular básica para validar estructura de email
    patron = r"^[a-zA-Z0-9_.+-]+@" + re.escape(dominio) + r"$"
    return re.match(patron, email.strip()) is not None
