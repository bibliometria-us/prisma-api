from abc import ABC, abstractmethod
from typing import Any, Type

from db.conexion import BaseDatos
from models.attribute import Attribute
from models.condition import Condition


class Model(ABC):
    def __init__(
        self,
        db_name: str,
        table_name: str,
        alias: str,
        primary_key: str,
        attributes: list[Attribute],
        components: list["Component"] = list(),
    ) -> None:
        self.db = BaseDatos(database=None)
        self.metadata = Metadata(
            db_name,
            table_name,
            alias,
            primary_key,
        )
        self.attribute_list = attributes
        self.attributes = self.index_attributes()
        self.component_list = components
        self.components: dict[str, Component] = {}
        self.load_components()

    def get(
        self,
        conditions: list[Condition] = None,
        all: bool = False,
        logical_operator: str = "AND",
    ) -> None:

        # Construir la sentencia SELECT con las columnas de la tabla asociada al objeto
        columns = ", ".join(
            f"{self.metadata.alias}.{attribute.column_name} as {attribute.display_name}"
            for attribute in self.attributes.values()
        )
        table_name = f"{self.metadata.db_name}.{self.metadata.table_name}"

        # Añadir JOINS para las relaciones 1..1

        query = f"""SELECT {columns} FROM {table_name} {self.metadata.alias}"""
        params = {}

        # CONDICIONES

        # Si no hay condiciones establecidas, se hace la búsqueda por primary key
        if not conditions:
            primary_key = self.get_primary_key()
            conditions = [
                Condition(
                    query=f"{primary_key.column_name} = %({primary_key.column_name})s"
                )
            ]
            params[primary_key.column_name] = primary_key.value

        # TODO: Implementar lógica de condiciones
        if not all:
            where = f" WHERE {f' {logical_operator} '.join((condition.generate_query() for condition in conditions))}"
            query += where

        result = self.db.ejecutarConsulta(query, params)

        if self.db.has_rows():
            self.store_data(result)
        else:
            self.clear_attributes()

        return result

    def create(self, attribute_filter=[]) -> None:
        self._add(attribute_filter=attribute_filter, insert_type="INSERT")

    def update(self, attribute_filter=[]) -> None:
        self._add(attribute_filter=attribute_filter, insert_type="REPLACE")

    def _add(self, attribute_filter=[], insert_type="") -> None:
        table_name = f"{self.metadata.db_name}.{self.metadata.table_name}"

        # Atributos que se van a utilizar para actualizar la consulta (por defecto todos)
        attributes = self.attributes

        # Los atributos que estén en attribute_filter no se introducen en el update para que se coloquen valores por defecto
        attributes = {
            key: value
            for key, value in attributes.items()
            if key not in attribute_filter
        }

        columns = list(attribute.column_name for attribute in attributes.values())

        param_ids = list(
            f"%({attribute.column_name})s" for attribute in attributes.values()
        )

        params = {key: value.value for key, value in self.attributes.items()}

        query = f"""{insert_type} INTO {table_name} ({','.join(columns)}) 
                    VALUES ({','.join(param_ids)})"""

        result = self.db.ejecutarConsulta(query, params)

        # Si se ha generado una nueva ID por auto_increment, se añade a la primary key
        if self.db.last_id:
            self.get_primary_key().value = self.db.last_id
        return None

    def update_attribute(self, attribute: str, value: Any) -> None:
        assert attribute in self.attributes.keys()

        self.set_attribute(key=attribute, value=value)

        query = f"""UPDATE {self.metadata.db_name}.{self.metadata.table_name} 
                SET {attribute} = %(value)s
                WHERE {self.get_primary_key().column_name} = %(primary_key)s"""

        params = {
            "value": self.get_attribute_value(attribute),
            "primary_key": self.get_primary_key().value,
        }

        self.db.ejecutarConsulta(query, params)

    def update_attributes(self, attributes: dict[str, Any]) -> None:
        for attribute, value in attributes.items():
            self.update_attribute(attribute, value)

    def delete(self, conditions=None) -> None:
        table_name = f"{self.metadata.db_name}.{self.metadata.table_name}"

        query = f"""DELETE FROM {table_name} WHERE """

        params = {}

        if not conditions:
            primary_key = self.get_primary_key()
            where = f"{primary_key.column_name} = %({primary_key.column_name})s"
            params[primary_key.column_name] = primary_key.value
        else:
            # TODO: Implementar lógica de condiciones
            where = ""

        query += where

        self.db.ejecutarConsulta(query, params)

    def clear_attributes(self):
        for key in self.attributes.keys():
            self.set_attribute(key, None)

    def get_attribute_value(self, attribute_name) -> Any:
        return self.attributes[attribute_name].value

    def set_attribute(self, key: str, value: Any) -> None:
        if key in self.attributes:
            self.attributes.get(key).set_value(value)
        else:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{key}'"
            )

    def set_attributes(self, values: dict[str, Any]) -> None:
        for key, value in values.items():
            self.set_attribute(key, value)

    def store_data(self, update) -> None:
        # TODO: Utilizar esta función para registrar cambios para logs
        for index in range(0, len(update[0])):
            key = update[0][index]
            value = update[1][index]
            self.set_attribute(key, value)

    def get_primary_key(self) -> Attribute:
        return self.attributes.get(self.metadata.primary_key)

    def index_attributes(self) -> dict[str, Attribute]:
        result = {attribute.column_name: attribute for attribute in self.attribute_list}

        return result

    def get_visible_attributes(self) -> list[Attribute]:
        return (
            attribute for attribute in self.attributes.values() if attribute.visible
        )

    def load_components(self) -> None:
        for component in self.component_list:
            self.load_component(component)

    def load_component(self, component: "Component") -> None:
        if not hasattr(self, component.name):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{component.name}'"
            )
        self.components[component.name] = component

    def enable_component(self, component_name: str) -> None:
        component = self.components.get(component_name)
        component.enabled = True

    def get_component(self, component_id: str) -> None:
        component = self.components.get(component_id)

        getter_name = component.getter

        if getter_name:
            getter = getattr(self, component.getter)

            if callable(getter):
                value = getter()

            else:

                raise AttributeError(
                    f"'{type(self).__name__}' object has no callable method '{getter}'"
                )

            setattr(self, component.name, value)
            component.value = value

        else:
            value = self.get_component_dynamic(component_id)

    def get_component_dynamic(self, component_id: str) -> "Model":

        pass

    def get_editable_columns(self) -> list[str]:
        return (
            key
            for key in self.attributes.keys()
            if key != self.get_primary_key().column_name
        )


class Metadata:
    def __init__(
        self,
        db_name: str,
        table_name: str,
        alias: str,
        primary_key: str,
    ) -> None:
        self.db_name = db_name
        self.table_name = table_name
        self.alias = alias
        self.primary_key = primary_key


class Component:
    def __init__(
        self,
        type: Type,
        name: str,
        cardinality: str,
        getter: str = None,
        target_table: str = None,
        intermediate_table: str = None,
        enabled: bool = False,
    ) -> None:
        self.type = type
        self.name = name
        self.getter = getter
        self.target_table = target_table
        self.intermediate_table = intermediate_table
        self.cardinality = cardinality
        self.enabled = enabled
        self.value: Model = self.type()
