import abc
import struct
from typing import List, Tuple

import attrs

from scaled.protocol.python.objects import MessageType, TaskStatus, TaskEchoStatus


class Message(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def serialize(self) -> Tuple[bytes, ...]:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize(data: List[bytes]):
        raise NotImplementedError()


@attrs.define
class Task(Message):
    task_id: bytes
    function_name: bytes
    function_args: bytes

    def serialize(self) -> Tuple[bytes, bytes, bytes]:
        return self.task_id, self.function_name, self.function_args

    @staticmethod
    def deserialize(data: List[bytes]):
        return Task(data[0], data[1], data[2])


@attrs.define
class TaskEcho(Message):
    task_id: bytes
    status: TaskEchoStatus

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskEcho(data[0], TaskEchoStatus(data[1]))


@attrs.define
class TaskCancel(Message):
    task_id: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id,

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskCancel(data[0])


@attrs.define
class TaskCancelEcho(Message):
    task_id: bytes
    status: TaskEchoStatus

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskCancelEcho(data[0], TaskEchoStatus(data[1]))


@attrs.define
class TaskResult(Message):
    task_id: bytes
    status: TaskStatus
    result: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value, self.result

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskResult(data[0], TaskStatus(data[1]), data[2])


@attrs.define
class Heartbeat(Message):
    identity: bytes
    cpu_usage: float

    def serialize(self) -> Tuple[bytes, ...]:
        return self.identity, struct.pack("f", self.cpu_usage)

    @staticmethod
    def deserialize(data: List[bytes]):
        return Heartbeat(data[0], struct.unpack("f", data[1])[0])


PROTOCOL = {
    MessageType.Heartbeat.value: Heartbeat,
    MessageType.Task.value: Task,
    MessageType.TaskEcho.value: TaskEcho,
    MessageType.TaskCancel.value: TaskCancel,
    MessageType.TaskCancelEcho.value: TaskCancelEcho,
    MessageType.TaskResult.value: TaskResult,
}
