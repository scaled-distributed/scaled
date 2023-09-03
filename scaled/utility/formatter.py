MODULUS = 1024.0


def format_bytes(number) -> str:
    for unit in ["b", "k", "m", "g", "t"]:
        if number >= MODULUS:
            number /= MODULUS
            continue

        if unit in {"b", "k"}:
            return f"{int(number)}{unit}"

        return f"{number:.1f}{unit}"


def format_integer(number):
    return f"{number:,}"


def format_percentage(number):
    return f"{number:.1%}"
