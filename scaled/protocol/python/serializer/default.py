import pickle
from typing import Any
from typing import Callable

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
    def serialize_argument(arg: Any) -> bytes:
        return pickle.dumps(arg, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize_argument(payload: bytes) -> Any:
        return pickle.loads(payload)

    @staticmethod
    def serialize_result(result: Any) -> bytes:
        return pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize_result(payload: bytes) -> Any:
        return pickle.loads(payload)
