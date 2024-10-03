from werkzeug.datastructures import FileStorage
from db.conexion import BaseDatos
from utils.format import flask_csv_to_df


def carga_acuerdos_transformativos():
    with open(
        "routes/carga/fuente/metricas/acuerdos_transformativos/fuentes/Listado revistas completo.csv",
        "rb",
    ) as file:
        file_storage = FileStorage(
            stream=file,
            name="Listado revistas completo.csv",
            content_type="application/octet-stream",
        )
        revistas = flask_csv_to_df(file_storage)

    db = BaseDatos()

    for index, revista in revistas.iterrows():
        query = """
        SELECT fuente.idFuente FROM p_fuente fuente
        LEFT JOIN (SELECT valor, idFuente FROM p_identificador_fuente WHERE tipo IN ('issn','eissn')) issn ON issn.idFuente = fuente.idFuente
        WHERE issn.valor IN (%(issn)s, %(eissn)s)
                """

        params = {
            "issn": revista["ISSN"],
            "eissn": revista["eISSN"],
        }

        result = db.ejecutarConsulta(query, params)
        found = db.has_rows()

        if found:
            id_fuente = result[1][0]
        else:
            query_revista = """
            INSERT INTO p_fuente (tipo, titulo, editorial, origen)
            VALUES (%(tipo)s, %(titulo)s, %(editorial)s, %(origen)s)
            """

            params_revista = {
                "tipo": "Revista",
                "titulo": revista["Título"],
                "editorial": revista["Editorial"],
                "origen": "ACUERDOS TRANSFORMATIVOS",
            }

            db.ejecutarConsulta(query_revista, params_revista)
            id_fuente = db.last_id

            query_issn = """
                        INSERT INTO p_identificador_fuente (idFuente, tipo, valor)
                        VALUES (%(idFuente)s, %(tipo)s, %(valor)s)
                        """
            for tipo in ("ISSN", "eISSN"):
                valor = revista[tipo]
                if not valor:
                    continue
                params_issn = {
                    "idFuente": id_fuente,
                    "tipo": tipo.lower(),
                    "valor": valor,
                }

                db.ejecutarConsulta(query_issn, params_issn)

        query_at = """
            REPLACE INTO m_at (idFuente, nombre, tipo, descuento, licencias_limitadas, promotor)
            VALUES (%(idFuente)s, %(nombre)s, %(tipo)s, %(descuento)s, %(licencias_limitadas)s, %(promotor)s)
            """

        licencias_limitadas = False
        if len(str(revista["Descuento"])) == 0:
            revista["Descuento"] = "0"
        if str(revista["Descuento"]).isnumeric():
            descuento = int(revista["Descuento"])
        else:
            descuento = int(
                "".join(c for c in str(revista["Descuento"]) if c.isdigit())
            )
            licencias_limitadas = True

        params_at = {
            "idFuente": id_fuente,
            "nombre": revista["Editorial"],
            "tipo": revista["Tipo"],
            "descuento": descuento,
            "licencias_limitadas": licencias_limitadas,
            "promotor": revista["Promotor"],
        }

        db.ejecutarConsulta(query_at, params_at)

        pass
