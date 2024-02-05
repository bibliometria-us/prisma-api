import os

def sort_logs(route):
    logs = [f for f in os.listdir(route) if f.endswith('.log')]
    paths = [os.path.join(route, log) for log in logs]
    sorted_logs = sorted(paths, key=lambda x: os.stat(x).st_ctime)

    return sorted_logs

def merge_logs(logs):
    result = ""

    for log in logs:
        with open(log, 'r') as file:
            result += "\n" + file.read()
    return result

def merge_sorted_logs(route):
    sorted_logs = sort_logs(route)
    merged_logs = merge_logs(sorted_logs)

    return merged_logs, sorted_logs
