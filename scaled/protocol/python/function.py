import ast
import importlib
import os
import pickle
from typing import Any, Callable, Tuple


class FunctionSerializer:
    @staticmethod
    def serialize_function(fn: Callable) -> bytes:
        return f"{fn.__module__}:{fn.__name__}".encode()

    @staticmethod
    def deserialize_function(payload: bytes) -> Callable:
        return load_function(payload)

    @staticmethod
    def serialize_arguments(args: Tuple[Any, ...]) -> bytes:
        return pickle.dumps(args)

    @staticmethod
    def deserialize_arguments(payload: bytes) -> Tuple[Any, ...]:
        return pickle.loads(payload)

    @staticmethod
    def serialize_result(result: Any) -> bytes:
        return pickle.dumps(result)

    @staticmethod
    def deserialize_result(payload: bytes) -> Any:
        return pickle.loads(payload)


def load_function(function_name: bytes) -> Callable:
    module, obj = function_name.decode().split(":", 1)
    try:
        mod = importlib.import_module(module)
    except ImportError:
        if module.endswith(".py") and os.path.exists(module):
            raise ImportError(f"Failed to find module, did you mean '{module.rsplit('.', 1)[0]}:{obj}'")
        raise

    try:
        expression = ast.parse(obj, mode="eval").body
    except SyntaxError:
        raise ImportError(f"Failed to parse {obj} as an attribute name or function call")

    if isinstance(expression, ast.Name):
        name = expression.id
    elif isinstance(expression, ast.Call):
        if not isinstance(expression.func, ast.Name):
            raise ImportError(f"Function reference must be a simple name: {obj}")

        name = expression.func.id
    else:
        raise ImportError(f"Failed to parse {obj} as attribute name or function call")

    try:
        func = getattr(mod, name)
    except AttributeError:
        raise ImportError(f"Failed to find attribute {name} in {module}")

    return func
