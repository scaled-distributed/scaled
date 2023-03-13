import abc
from typing import Any, Callable, Tuple, TypeVar


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
    def serialize_arguments(args: Tuple[Any, ...]) -> Tuple[bytes, ...]:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize_arguments(payload: Tuple[bytes, ...]) -> Tuple[Any, ...]:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def serialize_result(result: Any) -> bytes:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize_result(payload: bytes) -> Any:
        raise NotImplementedError()


FunctionSerializerType = TypeVar("FunctionSerializerType", bound=Serializer)
