from db.conexion import BaseDatos


def store_sso_data(sso_data: dict):
    db = BaseDatos(database="api")

    sso_data_string = {key: ", ".join(value) for key, value in sso_data.items()}

    query = f"""REPLACE INTO sso_data VALUES ('{sso_data_string.get('edupersonaffiliation')}',
                                            '{sso_data_string.get('givenname')}',
                                            '{sso_data_string.get('mail')}',
                                            '{sso_data_string.get('schacSn1') or sso_data_string.get('sn1')}',
                                            '{sso_data_string.get('schacSn2') or sso_data_string.get('sn2')}',
                                            '{sso_data_string.get('schacuserstatus')}',
                                            '{sso_data_string.get('uid')}',
                                            '{sso_data_string.get('usesrelacion')}'
                                            )"""

    result = db.ejecutarConsulta(query)

    return None
