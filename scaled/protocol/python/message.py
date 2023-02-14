import abc
import enum
import pickle
import struct
from typing import Dict, List, Tuple, TypeVar

import attrs


class MessageType(enum.Enum):
    Task = b"TK"
    TaskEcho = b"TE"
    TaskCancel = b"TC"
    TaskCancelEcho = b"TX"
    TaskResult = b"TR"

    Heartbeat = b"HB"

    FunctionRequest = b"FR"
    FunctionResponse = b"FA"

    MonitorRequest = b"MR"
    MonitorResponse = b"MS"

    @staticmethod
    def allowed_values():
        return {member.value for member in MessageType}


class TaskStatus(enum.Enum):
    Success = b"S"
    Failed = b"F"
    Canceled = b"C"


class TaskEchoStatus(enum.Enum):
    SubmitOK = b"SK"
    CancelOK = b"CK"
    Duplicated = b"DC"
    FunctionNotExists = b"FN"


class FunctionRequestType(enum.Enum):
    Check = b"C"
    Add = b"A"
    Request = b"R"
    Delete = b"D"


class FunctionResponseType(enum.Enum):
    OK = b"OK"
    NotExists = b"NE"
    StillHaveTask = b"HT"
    Duplicated = b"DC"


class _Message(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def serialize(self) -> Tuple[bytes, ...]:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize(data: List[bytes]):
        raise NotImplementedError()


MessageVariant = TypeVar("MessageVariant", bound=_Message)


@attrs.define
class Task(_Message):
    task_id: bytes
    function_id: bytes
    function_content: bytes
    function_args: bytes

    def serialize(self) -> Tuple[bytes, bytes, bytes, bytes]:
        return self.task_id, self.function_id, self.function_content, self.function_args

    @staticmethod
    def deserialize(data: List[bytes]):
        return Task(data[0], data[1], data[2], data[3])


@attrs.define
class TaskEcho(_Message):
    task_id: bytes
    status: TaskEchoStatus

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskEcho(data[0], TaskEchoStatus(data[1]))


@attrs.define
class TaskCancel(_Message):
    task_id: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.task_id,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskCancel(data[0])


@attrs.define
class TaskCancelEcho(_Message):
    task_id: bytes
    status: TaskEchoStatus

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskCancelEcho(data[0], TaskEchoStatus(data[1]))


@attrs.define
class TaskResult(_Message):
    task_id: bytes
    status: TaskStatus
    duration: float
    result: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value, struct.pack("f", self.duration), self.result

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskResult(data[0], TaskStatus(data[1]), struct.unpack("f", data[2])[0], data[3])


@attrs.define
class Heartbeat(_Message):
    cpu_usage: float
    rss_size: int

    def serialize(self) -> Tuple[bytes, ...]:
        return (struct.pack("fQ", self.cpu_usage, self.rss_size),)

    @staticmethod
    def deserialize(data: List[bytes]):
        return Heartbeat(*struct.unpack("fQ", data[0]))


@attrs.define
class MonitorRequest(_Message):
    def serialize(self) -> Tuple[bytes, ...]:
        return (b"",)

    @staticmethod
    def deserialize(data: List[bytes]):
        return MonitorRequest()


@attrs.define
class MonitorResponse(_Message):
    data: Dict

    def serialize(self) -> Tuple[bytes, ...]:
        return (pickle.dumps(self.data),)

    @staticmethod
    def deserialize(data: List[bytes]):
        return MonitorResponse(pickle.loads(data[0]))


@attrs.define
class FunctionRequest(_Message):
    type: FunctionRequestType
    function_id: bytes
    content: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.type.value, self.function_id, self.content

    @staticmethod
    def deserialize(data: List[bytes]):
        return FunctionRequest(FunctionRequestType(data[0]), data[1], data[2])


@attrs.define
class FunctionResponse(_Message):
    status: FunctionResponseType
    function_id: bytes
    content: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.status.value, self.function_id, self.content

    @staticmethod
    def deserialize(data: List[bytes]):
        return FunctionResponse(FunctionResponseType(data[0]), data[1], data[2])


PROTOCOL = {
    MessageType.Heartbeat.value: Heartbeat,
    MessageType.Task.value: Task,
    MessageType.TaskEcho.value: TaskEcho,
    MessageType.TaskCancel.value: TaskCancel,
    MessageType.TaskCancelEcho.value: TaskCancelEcho,
    MessageType.TaskResult.value: TaskResult,
    MessageType.MonitorRequest.value: MonitorRequest,
    MessageType.MonitorResponse.value: MonitorResponse,
    MessageType.FunctionRequest.value: FunctionRequest,
    MessageType.FunctionResponse.value: FunctionResponse,
}
