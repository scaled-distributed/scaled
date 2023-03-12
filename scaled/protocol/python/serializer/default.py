import pickle
from typing import Any, Callable, Tuple

import cloudpickle

from scaled.protocol.python.serializer.mixins import Serializer


class DefaultSerializer(Serializer):
    @staticmethod
    def serialize_function(fn: Callable) -> bytes:
        return cloudpickle.dumps(fn)

    @staticmethod
    def deserialize_function(payload: bytes) -> Callable:
        return cloudpickle.loads(payload)

    @staticmethod
    def serialize_arguments(args: Tuple[Any, ...]) -> Tuple[bytes, ...]:
        return tuple(pickle.dumps(arg, protocol=pickle.HIGHEST_PROTOCOL) for arg in args)

    @staticmethod
    def deserialize_arguments(payload: Tuple[bytes, ...]) -> Tuple[Any, ...]:
        return tuple(pickle.loads(arg) for arg in payload)

    @staticmethod
    def serialize_result(result: Any) -> bytes:
        return pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize_result(payload: bytes) -> Any:
        return pickle.loads(payload)
