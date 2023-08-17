import math


def get_page_offset(page, page_size, amount):
    if page_size == 0:
        page_size = amount

    min_page = 1
    max_page = math.ceil(amount/page_size) if page_size > 0 else 1

    # Limitar el número de página entre el mínimo y el máximo posible
    page = constrain_to_range(page, min_page, max_page)

    offset = min((page-1)*page_size, amount)

    return page_size, offset


def constrain_to_range(value, min_value, max_value):
    return max(min(value, max_value), min_value)
