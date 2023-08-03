import csv
import io
import json
import xml.etree.ElementTree as ET
from collections import OrderedDict
from utils.timing import func_timer as timer


def format_csv(data):
    csv_string = ''
    if len(data) > 0:
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer, delimiter=',')

        writer.writerows(data)

        csv_string = csv_buffer.getvalue()

        csv_buffer.close()

    return csv_string


@timer
def dict_from_table(data, selectable_column, base_name="", nested: dict = {}):
    result = {}

    selectable_column_index = data[0].index(selectable_column)

    for row in data[1:]:
        # Dato del diccionario
        data_dict = {data[0][i]: value for i, value in enumerate(row)}

        # Nombre de la clave
        row_name: str = base_name + "_" + str(row[selectable_column_index])

        result_data_dict = {}
        for d_data in data_dict:
            # Separamos el nombre de la clave por _ y nos quedamos con el primer elemento
            suffix = d_data.split("_")[0]
            value = data_dict[d_data]
            # Si el sufijo está en el diccionario nested, significa que va a pertenecer a un diccionario anidado al principal
            if suffix in nested:
                group_name = nested[suffix]
                if group_name in result_data_dict:
                    pass
                else:
                    result_data_dict[group_name] = {}

                result_data_dict[group_name][d_data.replace(
                    f"{suffix}_", "")] = value
            else:
                result_data_dict[d_data] = value

        result[row_name] = result_data_dict

    return dict(sorted(result.items(), key=lambda item: item[1][selectable_column]))


@timer
def dict_to_xml(data, root_name=None, object_name=""):
    root = ET.Element(root_name)

    def _dict_to_xml(parent, data):
        for key, value in data.items():
            if isinstance(value, dict):
                # For nested dictionaries, create a new XML element
                if isinstance(key, str) and key.startswith(object_name):
                    element_key = object_name[0:len(object_name)]
                else:
                    element_key = str(key)
                element = ET.SubElement(parent, element_key)
                _dict_to_xml(element, value)
            else:
                # For non-dictionary values, create a new XML element and set its text
                element_key = str(key)
                ET.SubElement(parent, element_key).text = str(value)

    # Convert the dictionary to XML recursively
    _dict_to_xml(root, data)

    # Create the ElementTree from the root element
    xml_tree = ET.ElementTree(root)

    # Return the XML string representation
    return ET.tostring(root, encoding="utf-8", method="xml").decode("utf-8")


def dict_to_json(data):
    result = json.dumps(data, indent=4, sort_keys=False)
    return result
