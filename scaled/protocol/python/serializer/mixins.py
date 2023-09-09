import abc
from typing import Any
from typing import Callable


class Serializer(metaclass=abc.ABCMeta):
    @staticmethod
    @abc.abstractmethod
    def serialize_function(fn: Callable) -> bytes:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize_function(payload: bytes) -> Callable:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def serialize_argument(args: Any) -> bytes:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize_argument(payload: bytes) -> Any:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def serialize_result(result: Any) -> bytes:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize_result(payload: bytes) -> Any:
        raise NotImplementedError()
