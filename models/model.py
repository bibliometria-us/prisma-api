from abc import ABC, abstractmethod
from typing import Any, Callable, List, Type

from pandas import DataFrame

from db.conexion import BaseDatos
from models.attribute import Attribute
from models.condition import Condition
from utils.format import table_to_pandas


class Model(ABC):
    def __init__(
        self,
        db_name: str,
        table_name: str,
        alias: str,
        primary_key: str,
        attributes: list[Attribute],
        components: list["Component"] = list(),
        enabled_components: list[str] = list(),
        values: list["Model"] = None,
        db: BaseDatos = None,
    ) -> None:
        self.db = db or BaseDatos(database=None)
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
        self.enabled_components = enabled_components
        self.load_components()
        self.set_enabled_components()
        self.values = values or []

    def get(
        self,
        conditions: list[Condition] = None,
        all: bool = False,
        multiple: bool = False,
        multiple_components: list = [],
        logical_operator: str = "AND",
    ) -> None:

        # Construir la sentencia SELECT con las columnas de la tabla asociada al objeto
        column_list = list(
            f"{self.metadata.alias}.{attribute.column_name} as {attribute.display_name}"
            for attribute in self.attributes.values()
        )

        table_name = f"{self.metadata.db_name}.{self.metadata.table_name}"

        # Añadir JOINS  y columnas para las relaciones 1..1

        join_list = list()

        for name, component in self.components.items():

            if not component.cardinality == "single":
                continue

            if not component.intermediate_table:
                join = f"""
                LEFT JOIN {component.db_name}.{component.target_table} {name}
                    ON {name}.{component.foreign_target_column} = {self.metadata.alias}.{component.foreign_key}
                """
                join_list.append(join)

                column_list += list(
                    f"{name}.{attribute.column_name} as {name}___{attribute.column_name}"
                    for attribute in component.value.attributes.values()
                )

        # Preparar consulta

        columns = ", ".join(column_list)
        joins = " ".join(join_list)
        query = f"""SELECT {columns} FROM {table_name} {self.metadata.alias}
                {joins}"""
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

        query_result = self.db.ejecutarConsulta(query, params)

        result = None

        if self.db.has_rows():
            result = self.store_data(query_result, multiple, multiple_components)
            self.get_enabled_components()
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

        # Componentes obligatorios 1..1

        mandatory_foreign_components = {
            component.foreign_key: component.value.get_attribute_value(
                component.foreign_key
            )
            for component in self.components.values()
            if component.is_single_mandatory()
        }

        columns += list(name for name in mandatory_foreign_components.keys())

        param_ids += list(f"%({name})s" for name in mandatory_foreign_components.keys())

        params.update(mandatory_foreign_components)

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

    def set_attributes(self, values: dict[str, Any]) -> None:
        for key, value in values.items():
            self.set_attribute(key, value)

    def store_data(
        self, update, multiple: bool, multiple_components: list[str]
    ) -> None:
        df = table_to_pandas(update)

        if not multiple:
            for index, row in df.head(1).iterrows():
                for key, value in row.items():
                    if len(key.split("___")) == 1:
                        self.set_attribute(key, value)
                    else:
                        component_name = key.split("___")[0]
                        component_attribute = key.split("___")[1]
                        self.components.get(component_name).value.set_attribute(
                            component_attribute, value
                        )

            return self

        else:
            result = []
            for index, row in df.iterrows():
                item = self.__class__()
                for key, value in row.items():
                    item.set_attribute(key, value)

                # item.get_enabled_components()
                result.append(item)
            self.values = result
            return result

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

        self.components[component.name] = component

    def enable_component(self, component_name: str) -> None:
        component = self.components.get(component_name)
        component.enabled = True

    def get_enabled_components(self) -> None:
        for component_id, component in self.components.items():
            if component.enabled:
                self.get_component(component_id)

    def set_enabled_components(self) -> None:
        if not self.enabled_components:
            return None

        for name, component in self.components.items():
            if name in self.enabled_components:
                component.enable()
            else:
                component.disable()

    def get_component(self, component_id: str) -> None:
        component = self.components.get(component_id)

        getter_name = component.getter

        if getter_name:
            self.get_component_by_getter(component)

        else:
            self.get_component_dynamically(component)

    def set_component(self, component_id: str, value: dict) -> None:
        component = self.components.get(component_id)

        component.value.set_attributes(value)

    def set_components(self, dict: dict) -> None:
        for component_id, value in dict.items():
            self.set_component(component_id=component_id, value=value)

    def update_component(self, component_id: str) -> None:
        component = self.components.get(component_id)
        assert component.cardinality == "single"

        if not component.intermediate_table:
            pass

        else:
            query = f"""UPDATE {component.intermediate_table_db_name}.{component.intermediate_table}
                    SET {component.intermediate_table_component_key} = %(value)s
                    WHERE {self.get_primary_key().column_name} = {self.get_primary_key().value}
                    """
            params = {
                "value": component.value.get_attribute_value(
                    component.foreign_target_column
                )
            }

        self.db.ejecutarConsulta(query, params)

    def _update_component(self, component: "Component"):

        query = f"""UPDATE {self.metadata.db_name}.{self.metadata.table_name}
                    SET {component.foreign_key} = %(value)s
                    WHERE {self.get_primary_key().column_name} = {self.get_primary_key().value}
                    """
        params = {
            "value": component.value.get_attribute_value(
                component.foreign_target_column
            )
        }

    def get_component_by_getter(self, component: "Component") -> "Model":
        getter = getattr(self, component.getter)

        if callable(getter):
            value = getter()

        else:
            raise AttributeError(
                f"'{type(self).__name__}' object has no callable method '{getter}'"
            )

        component.value = value

        return component.value

    def get_component_dynamically(self, component: "Component") -> "Model":

        select = component.value.get_select_columns_mapping(prefix=component.name)
        primary_key = self.get_primary_key()

        if not component.intermediate_table:
            sql = f"""SELECT {select} FROM {component.db_name}.{component.target_table} AS {component.name}
                        LEFT JOIN  {self.metadata.db_name}.{self.metadata.table_name} AS {self.metadata.alias} 
                        ON {self.metadata.alias}.{component.foreign_key} = {component.name}.{component.foreign_key}
                        WHERE {self.metadata.alias}.{primary_key.column_name} = {primary_key.value}
                    """

        else:
            sql = f"""SELECT {select} FROM {component.db_name}.{component.target_table} AS {component.name}
                        LEFT JOIN {component.intermediate_table_db_name}.{component.intermediate_table} AS {component.intermediate_table_alias}
                        ON {component.intermediate_table_alias}.{component.intermediate_table_component_key} 
                            = {component.name}.{component.foreign_key}
                        LEFT JOIN {self.metadata.db_name}.{self.metadata.table_name} AS {self.metadata.alias} 
                        ON {self.metadata.alias}.{primary_key.column_name} 
                            = {component.intermediate_table_alias}.{component.intermediate_table_key}
            
                        WHERE {self.metadata.alias}.{primary_key.column_name} = {primary_key.value}
                    """

        result = self.db.ejecutarConsulta(sql)

        df = table_to_pandas(result)
        component.value.load_from_dataframe(
            df=df,
            cardinality=component.cardinality,
        )

        pass

    def update_single_component(self, component_id: str, value: Any) -> None:
        component = self.components.get(component_id)

        if component.cardinality != "single":
            raise Exception

        if component.intermediate_table == None:
            sql = f"""
                UPDATE {self.metadata.db_name}.{self.metadata.table_name}
                SET {component.foreign_key} = {value}
                WHERE {self.get_primary_key().column_name} = {self.get_primary_key().value}
                """

        else:
            pass

        self.db.ejecutarConsulta(sql)

    def get_editable_columns(self) -> list[str]:
        return (
            key
            for key in self.attributes.keys()
            if key != self.get_primary_key().column_name
        )

    # Devuelve la lista de columnas y alias para introducirse dentro de un SELECT, en base a los atributos del objeto
    def get_select_columns_mapping(self, prefix: str) -> str:
        return ", ".join(
            f"{prefix}.{attribute.column_name} as {attribute.display_name}"
            for attribute in self.attributes.values()
        )

    def load_from_dataframe(self, df: DataFrame, cardinality: str):
        if cardinality == "single":
            pd_dict = df.iloc[0].to_dict()
            self.set_attributes(pd_dict)
        if cardinality == "many":
            model_attributes_list = df.to_dict(orient="records")
            for model_attributes in model_attributes_list:
                model = self.__class__()
                model.set_attributes(model_attributes)
                self.values.append(model)

    # Devuelve si es un resultado individual o múltiple
    def is_multiple(self):
        return len(self.values) > 0

    def to_dict(self, get_components=True):
        if not self.is_multiple():
            attribute_dict = {
                name: attribute.value for name, attribute in self.attributes.items()
            }
        else:
            attribute_dict = {
                index: attribute.to_dict()
                for index, attribute in enumerate((value for value in self.values))
            }

        if get_components:
            component_dict = {
                name: component.value.to_dict(get_components=False)
                for name, component in self.components.items()
                if component.value and component.value.get_primary_key().value
            }
        else:
            component_dict = {}

        return {**attribute_dict, **component_dict}


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
        type: Type[Model],
        db_name: str,
        name: str,
        cardinality: str,
        getter: str = None,
        target_table: str = None,
        foreign_key: str = None,
        foreign_target_column: str = None,
        intermediate_table: str = None,
        intermediate_table_db_name: str = None,
        intermediate_table_alias: str = None,
        intermediate_table_key: str = None,
        intermediate_table_component_key: str = None,
        enabled: bool = False,
        nullable: bool = True,
    ) -> None:
        self.type = type
        self.db_name = db_name
        self.name = name
        self.getter = getter
        self.target_table = target_table
        self.foreign_key = foreign_key
        self.foreign_target_column = foreign_target_column
        self.intermediate_table = intermediate_table
        self.intermediate_table_db_name = intermediate_table_db_name
        self.intermediate_table_alias = intermediate_table_alias
        self.intermediate_table_key = intermediate_table_key
        self.intermediate_table_component_key = intermediate_table_component_key
        self.cardinality = cardinality
        self.enabled = enabled
        self.value: Model = self.type()
        self.nullable = nullable

    def enable(self):
        self.enabled = True

    def disable(self):
        self.enabled = False

    def is_single_mandatory(self):
        return (
            not self.nullable
            and self.cardinality == "single"
            and not self.intermediate_table
        )
