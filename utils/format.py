import csv
import io
import json
import xml.etree.ElementTree as ET


def format_csv(data):
    csv_string = ''
    if len(data) > 0:
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer, delimiter=',')

        writer.writerows(data)

        csv_string = csv_buffer.getvalue()

        csv_buffer.close()

    return csv_string


def dict_from_table(data, selectable_column):
    result = {}

    selectable_column_index = data[0].index(selectable_column)

    for row in data[1:]:
        data_dict = {data[0][i]: value for i, value in enumerate(row)}
        result[row[selectable_column_index]] = data_dict

    return result


def dict_to_xml(data, root_name=None):
    root = ET.Element(None)

    def _dict_to_xml(parent, data):
        for key, value in data.items():
            if isinstance(value, dict):
                # For nested dictionaries, create a new XML element
                if isinstance(key, int) and root_name:
                    element_key = f"{root_name}"
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
