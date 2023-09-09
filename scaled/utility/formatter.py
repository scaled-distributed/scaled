from typing import Union

STORAGE_SIZE_MODULUS = 1024.0
TIME_MODULUS = 1000


def format_bytes(number) -> str:
    for unit in ["b", "k", "m", "g", "t"]:
        if number >= STORAGE_SIZE_MODULUS:
            number /= STORAGE_SIZE_MODULUS
            continue

        if unit in {"b", "k"}:
            return f"{int(number)}{unit}"

        return f"{number:.1f}{unit}"

    raise ValueError("Too big to show the byte size that >1000t")


def format_integer(number) -> str:
    return f"{number:,}"


def format_percentage(number: float) -> str:
    return f"{number:.1%}"


def format_microseconds(number: Union[int, float]) -> str:
    for unit in ["us", "ms", "s"]:
        if number >= TIME_MODULUS:
            number /= TIME_MODULUS
            continue

        if unit == "us":
            return f"{number/TIME_MODULUS:.1f}ms"

        too_big_sign = "+" if unit == "s" and number > TIME_MODULUS else ""
        return f"{int(number)}{too_big_sign}{unit}"

    raise ValueError("Too big to show the seconds that >1000s")


def format_seconds(number: int) -> str:
    if number > 60:
        return "60+s"

    return f"{number}s"
