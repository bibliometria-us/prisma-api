import datetime
import string
import random
from typing import Iterable


def random_str(
    min_length,
    max_length,
    charset: list[str] = None,
    capitalize=False,
    space=False,
):
    if not charset:
        charset = ["lowercase", "uppercase"]

    charsets = {
        "lowercase": string.ascii_lowercase,
        "uppercase": string.ascii_uppercase,
        "digits": string.digits,
        "punctuation": string.punctuation,
    }

    characters = "".join(ch for name, ch in charsets.items() if name in charset)

    if space:
        characters += " "

    length = random.randint(min_length, max_length)

    result = "".join(random.choice(characters) for _ in range(length))

    if capitalize:
        result = result.capitalize()

    result = result.strip()
    return result


def random_int(
    min,
    max,
):

    result = random.randint(min, max)

    return result


def random_date(
    start_date,
    end_date=datetime.date.today(),
):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)

    return start_date + datetime.timedelta(days=random_days)


def random_datetime(
    start_datetime: datetime.datetime, end_datetime=datetime.datetime.now()
):
    # Convert start and end times to seconds since midnight
    # Convert start and end datetime to timestamps (seconds since epoch)
    start_timestamp = start_datetime.timestamp()
    end_timestamp = end_datetime.timestamp()

    # Generate a random timestamp within the range
    random_timestamp = random.uniform(start_timestamp, end_timestamp)

    # Convert the random timestamp back to a datetime object
    return datetime.datetime.fromtimestamp(random_timestamp)


def random_element(collection: Iterable):
    collection = list(collection)

    return random.choice(collection)


def random_bool():
    return random_element([True, False])


def random_bool_int():
    return random_element([0, 1])


def random_dni():
    number = random_int(11111111, 99999999)
    letters = [
        "T",
        "R",
        "W",
        "A",
        "G",
        "M",
        "Y",
        "F",
        "P",
        "D",
        "X",
        "B",
        "N",
        "J",
        "Z",
        "S",
        "Q",
        "V",
        "H",
        "L",
        "C",
        "K",
        "E",
    ]

    letter = letters[number % 23]
    result = str(number) + letter

    return result


print(random_bool())
