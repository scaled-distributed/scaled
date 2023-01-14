import struct
from typing import Tuple, List, Dict

import attrs

from scaled.protocol.python.objects import MessageType, TaskStatus
from scaled.protocol.python.serializer import Serializer


@attrs.define
class Task(Serializer):
    task_id: bytes
    function_name: bytes
    function_args: Tuple[bytes, ...]

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.function_name, *self.function_args

    @staticmethod
    def deserialize(data: List[bytes]):
        return Task(data[0], data[1], tuple(data[2:]))


@attrs.define
class TaskCancel(Serializer):
    task_id: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.task_id,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskCancel(data[0])


@attrs.define
class TaskResult(Serializer):
    task_id: bytes
    status: TaskStatus
    result: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value, *self.result

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskResult(data[0], TaskStatus(data[1]), data[2])


@attrs.define
class TaskEcho(Serializer):
    task_id: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.task_id,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskEcho(struct.unpack("l", data[0])[0])


@attrs.define
class Heartbeat(Serializer):
    identity: bytes
    cpu_usage: float

    def serialize(self) -> Tuple[bytes, ...]:
        return (
            self.identity,
            struct.pack("f", self.cpu_usage),
        )

    @staticmethod
    def deserialize(data: List[bytes]):
        return Heartbeat(data[0], struct.unpack("f", data[1])[0])


PROTOCOL = {
    MessageType.Heartbeat.value: Heartbeat,
    MessageType.Task.value: Task,
    MessageType.TaskEcho.value: TaskEcho,
    MessageType.TaskResult.value: TaskResult,
}
