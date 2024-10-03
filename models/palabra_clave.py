from models.attribute import Attribute
from models.colectivo.exceptions.exceptions import (
    LimitePalabrasClave,
    PalabraClaveDuplicada,
)
from models.model import Model
from utils.format import table_to_pandas


class PalabraClave(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_palabra_clave",
        alias="palabra_clave",
        primary_key="idPalabraClave",
    ):
        attributes = [
            Attribute(column_name="idPalabraClave"),
            Attribute(column_name="nombre"),
        ]
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
        )


def get_palabras_clave(model: Model):
    palabras_clave_component = model.components["palabras_clave"]

    tabla_intermedia = palabras_clave_component.intermediate_table
    nombre_id = model.metadata.primary_key
    valor_id = model.get_primary_key().value

    query = f"""SELECT pc.* FROM prisma.i_palabra_clave pc
            LEFT JOIN prisma.{tabla_intermedia} ti ON ti.idPalabraClave = pc.idPalabraClave
            WHERE ti.{nombre_id} = %(valor_id)s
            """

    params = {
        "valor_id": valor_id,
    }

    query_result = model.db.ejecutarConsulta(query, params)

    pd = table_to_pandas(query_result)

    model.palabras_clave = []
    for index, row in pd.iterrows():
        palabra_clave = PalabraClave()
        palabra_clave.set_attributes(
            {
                "idPalabraClave": row["idPalabraClave"],
                "nombre": row["nombre"],
            }
        )
        model.palabras_clave.append(palabra_clave)


def add_palabra_clave(
    model: Model, id_palabra_clave=None, nombre_palabra_clave=None
) -> PalabraClave:
    # Comprobar máximo de palabras clave
    model.get_palabras_clave()

    try:
        assert len(model.palabras_clave) < model.max_palabras_clave
    except AssertionError:
        raise LimitePalabrasClave(model.max_palabras_clave)

    palabras_clave_component = model.components["palabras_clave"]

    if not id_palabra_clave:
        nueva_palabra_clave = PalabraClave()
        nueva_palabra_clave.set_attribute("nombre", nombre_palabra_clave)
        nueva_palabra_clave.create()
        id_palabra_clave = nueva_palabra_clave.get_primary_key().value

    tabla_intermedia = palabras_clave_component.intermediate_table
    nombre_id = model.metadata.primary_key
    valor_id = model.get_primary_key().value

    query = f"""
            INSERT INTO prisma.{tabla_intermedia} (idPalabraClave, {nombre_id})
            VALUES (%(id_palabra_clave)s, %(valor_id)s)
            """

    params = {
        "id_palabra_clave": id_palabra_clave,
        "valor_id": valor_id,
    }

    model.db.ejecutarConsulta(query, params)

    # Comprobar que se ha insertado la palabra clave
    try:
        assert model.db.rowcount > 0
    except AssertionError:
        raise PalabraClaveDuplicada()

    result = PalabraClave()
    result.get_primary_key().value = id_palabra_clave
    result.get()

    return result


def remove_palabra_clave(model: Model, id_palabra_clave):
    palabras_clave_component = model.components["palabras_clave"]

    tabla_intermedia = palabras_clave_component.intermediate_table
    nombre_id = model.metadata.primary_key
    valor_id = model.get_primary_key().value

    query = f"""DELETE FROM prisma.{tabla_intermedia} WHERE 
              idPalabraClave = %(id_palabra_clave)s AND {nombre_id} = %(valor_id)s"""

    params = {
        "id_palabra_clave": id_palabra_clave,
        "valor_id": valor_id,
    }

    model.db.ejecutarConsulta(query, params)
