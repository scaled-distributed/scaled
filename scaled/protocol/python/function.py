import pickle
from typing import Any, Callable, Tuple

import cloudpickle


class FunctionSerializer:
    @staticmethod
    def serialize_function(fn: Callable) -> bytes:
        return cloudpickle.dumps(fn)

    @staticmethod
    def deserialize_function(payload: bytes) -> Callable:
        return cloudpickle.loads(payload)

    @staticmethod
    def serialize_arguments(args: Tuple[Any, ...]) -> bytes:
        return pickle.dumps(args, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize_arguments(payload: bytes) -> Tuple[Any, ...]:
        return pickle.loads(payload)

    @staticmethod
    def serialize_result(result: Any) -> bytes:
        return pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize_result(payload: bytes) -> Any:
        return pickle.loads(payload)
