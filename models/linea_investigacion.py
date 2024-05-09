from models.attribute import Attribute
from models.colectivo.exceptions.exceptions import LineaInvestigacionDuplicada
from models.model import Model
from utils.format import table_to_pandas


class LineaInvestigacion(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_linea_investigacion",
        alias="linea_investigacion",
        primary_key="idLineaInvestigacion",
    ):
        attributes = [
            Attribute(column_name="idLineaInvestigacion"),
            Attribute(column_name="nombre"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )


def get_lineas_investigacion(model: Model):

    lineas_investigacion_component = model.components["lineas_investigacion"]

    tabla_intermedia = lineas_investigacion_component.intermediate_table
    nombre_id = model.metadata.primary_key
    valor_id = model.get_primary_key().value

    query = f"""SELECT pc.* FROM prisma.i_linea_investigacion pc
            LEFT JOIN prisma.{tabla_intermedia} ti ON ti.idLineaInvestigacion = pc.idLineaInvestigacion
            WHERE ti.{nombre_id} = %(valor_id)s
            """

    params = {
        "valor_id": valor_id,
    }

    query_result = model.db.ejecutarConsulta(query, params)

    pd = table_to_pandas(query_result)

    model.lineas_investigacion = []
    for index, row in pd.iterrows():
        linea_investigacion = LineaInvestigacion()
        linea_investigacion.set_attributes(
            {
                "idLineaInvestigacion": row["idLineaInvestigacion"],
                "nombre": row["nombre"],
            }
        )
        model.lineas_investigacion.append(linea_investigacion)

    def add_linea_investigacion(
        self, id_linea_investigacion=None, nombre_linea_investigacion=None
    ):

        lineas_investigacion_component = self.components["lineas_investigacion"]

        if not id_linea_investigacion:
            nueva_linea_investigacion = LineaInvestigacion()
            nueva_linea_investigacion.set_attribute(
                "nombre", nombre_linea_investigacion
            )
            nueva_linea_investigacion.create()
            id_linea_investigacion = nueva_linea_investigacion.get_primary_key().value

        tabla_intermedia = lineas_investigacion_component.intermediate_table
        nombre_id = self.metadata.primary_key
        valor_id = self.get_primary_key().value

        query = f"""
                INSERT INTO prisma.{tabla_intermedia} (idLineaInvestigacion, {nombre_id})
                VALUES (%(id_linea_investigacion)s, %(valor_id)s)
                """

        params = {
            "id_linea_investigacion": id_linea_investigacion,
            "valor_id": valor_id,
        }

        self.db.ejecutarConsulta(query, params)

        # Comprobar que se ha insertado la palabra clave
        try:
            assert self.db.rowcount > 0
        except AssertionError:
            raise LineaInvestigacionDuplicada()

        result = LineaInvestigacion()
        result.get_primary_key().value = id_linea_investigacion
        result.get()

        return result


def add_linea_investigacion(
    model: Model, id_linea_investigacion=None, nombre_linea_investigacion=None
) -> LineaInvestigacion:

    lineas_investigacion_component = model.components["lineas_investigacion"]

    if not id_linea_investigacion:
        nueva_linea_investigacion = LineaInvestigacion()
        nueva_linea_investigacion.set_attribute("nombre", nombre_linea_investigacion)
        nueva_linea_investigacion.create()
        id_linea_investigacion = nueva_linea_investigacion.get_primary_key().value

    tabla_intermedia = lineas_investigacion_component.intermediate_table
    nombre_id = model.metadata.primary_key
    valor_id = model.get_primary_key().value

    query = f"""
            INSERT INTO prisma.{tabla_intermedia} (idLineaInvestigacion, {nombre_id})
            VALUES (%(id_linea_investigacion)s, %(valor_id)s)
            """

    params = {
        "id_linea_investigacion": id_linea_investigacion,
        "valor_id": valor_id,
    }

    model.db.ejecutarConsulta(query, params)

    # Comprobar que se ha insertado la palabra clave
    try:
        assert model.db.rowcount > 0
    except AssertionError:
        raise LineaInvestigacionDuplicada()

    result = LineaInvestigacion()
    result.get_primary_key().value = id_linea_investigacion
    result.get()

    return result


def remove_linea_investigacion(model: Model, id_linea_investigacion):
    lineas_investigacion_component = model.components["lineas_investigacion"]

    tabla_intermedia = lineas_investigacion_component.intermediate_table
    nombre_id = model.metadata.primary_key
    valor_id = model.get_primary_key().value

    query = f"""DELETE FROM prisma.{tabla_intermedia} WHERE 
              idLineaInvestigacion = %(id_linea_investigacion)s AND {nombre_id} = %(valor_id)s"""

    params = {
        "id_linea_investigacion": id_linea_investigacion,
        "valor_id": valor_id,
    }

    model.db.ejecutarConsulta(query, params)
