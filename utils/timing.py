import time
import logging


def func_timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info(
            f"Function '{func.__name__}' took {execution_time:.6f} seconds to run."
        )
        print(f"Function '{func.__name__}' took {execution_time:.6f} seconds to run.")
        return result

    return wrapper
