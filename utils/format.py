import csv
import io


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
