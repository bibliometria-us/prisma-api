import datetime


def get_current_year() -> int:
    return datetime.datetime.now().year


def get_current_date(format=False, format_str="%d-%m-%Y"):
    date = datetime.datetime.now()
    if format:
        return date.strftime(format_str)
    else:
        return date


def str_to_date(date, format="%d-%m-%Y"):
    if date:
        return datetime.datetime.strptime(date, format)


def date_to_str(date, format="%d-%m-%Y"):
    return date.strftime(format)
