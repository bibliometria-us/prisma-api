from utils import format
from flask import Response
from flask_restx import Namespace


def generate_response(data: list, output_types: list[str], accept_type: str, nested: dict,
                      namespace: Namespace, dict_selectable_column: str = None, object_name: str = None,
                      xml_root_name: str = None) -> Response:

    if 'json' in accept_type and 'json' in output_types:
        dict_data = format.dict_from_table(
            data, dict_selectable_column, object_name, nested)
        json_data = format.dict_to_json(dict_data)
        response = Response(json_data, mimetype='application/json')
        return response

    elif 'xml' in accept_type and 'xml' in output_types:
        dict_data = format.dict_from_table(
            data, dict_selectable_column, object_name, nested)
        xml_data = format.dict_to_xml(
            dict_data, root_name=xml_root_name, object_name=object_name)
        response = Response(xml_data, mimetype='application/xml')
        return response

    elif 'csv' in accept_type and 'csv' in output_types:
        csv_data = format.format_csv(data)
        return Response(csv_data, mimetype='text/csv')

    else:
        namespace.abort(406, 'Formato de salida no soportado')
