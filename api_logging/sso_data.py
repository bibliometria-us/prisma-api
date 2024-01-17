from db.conexion import BaseDatos

def store_sso_data(sso_data: dict):
    db = BaseDatos(database="api")

    sso_data_string = {key : ", ".join(value) for key, value in sso_data.items()}

    query = f"""INSERT INTO sso_data VALUES ('{sso_data_string['edupersonaffiliation']}',
                                            '{sso_data_string['givenname']}',
                                            '{sso_data_string['mail']}',
                                            '{sso_data_string['schacSn1']}',
                                            '{sso_data_string['schacSn2']}',
                                            '{sso_data_string['schacuserstatus']}',
                                            '{sso_data_string['uid']}',
                                            '{sso_data_string['usesrelacion']}'
                                            )"""
    
    result = db.ejecutarConsulta(query)

    return None
    
