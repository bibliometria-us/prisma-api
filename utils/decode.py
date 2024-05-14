import urllib.parse


def http_arg_decode(element: str) -> str:
    return urllib.parse.unquote(element)
