from security.check_users import es_admin, pertenece_a_conjunto


def comprobar_permisos(fuentes):
    if es_admin():
        return True
    for tipo_fuente, valor in fuentes.items():
        if tipo_fuente == "investigadores" and valor is not None:
            raise Exception
        if valor:
            assert pertenece_a_conjunto(tipo_fuente, valor)
