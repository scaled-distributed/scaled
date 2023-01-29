import functools
import logging
import typing

from home_library_common.logging.scoped_logger import ScopedLogger
from home_library_common.logging.utility import get_caller_location


def log_function(level_number: int = 2, logging_level: int = logging.INFO) -> typing.Callable:
    def decorator(func: typing.Callable) -> typing.Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with ScopedLogger(
                f"execute {func.__name__} at {get_caller_location(level_number)}", logging_level=logging_level
            ):
                return func(*args, **kwargs)

        return wrapper

    return decorator
