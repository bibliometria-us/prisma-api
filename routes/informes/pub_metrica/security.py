from security.check_users import es_admin, pertenece_a_conjunto, es_editor


def comprobar_permisos(fuentes, api_key):
    if es_admin(api_key=api_key):
        return True
    if es_editor(api_key=api_key):
        return True
    for tipo_fuente, valor in fuentes.items():
        if tipo_fuente == "investigadores" and valor is not None:
            raise Exception
        if valor:
            if len(valor.split(",")) > 1:
                raise Exception
            assert pertenece_a_conjunto(tipo_fuente, valor, api_key=api_key)
