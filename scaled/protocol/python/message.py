import abc
import dataclasses
import enum
import struct
from typing import Dict, List, Tuple, TypeVar

import bidict


class MessageType(enum.Enum):
    Task = b"TK"
    TaskEcho = b"TE"
    TaskCancel = b"TC"
    TaskResult = b"TR"

    GraphTask = b"GT"
    GraphTaskEcho = b"GE"
    GraphTaskCancel = b"GC"
    GraphTaskCancelEcho = b"GX"
    GraphTaskResult = b"GR"

    BalanceRequest = b"BQ"
    BalanceResponse = b"BR"

    Heartbeat = b"HB"

    FunctionRequest = b"FR"
    FunctionResponse = b"FA"

    SchedulerStatus = b"MS"

    DisconnectRequest = b"DR"
    DisconnectResponse = b"DP"

    ProcessorInitialize = b"PI"

    @staticmethod
    def allowed_values():
        return {member.value for member in MessageType}


class TaskStatus(enum.Enum):
    Success = b"S"
    Failed = b"F"
    Canceled = b"C"
    NotFound = b"N"


class TaskEchoStatus(enum.Enum):
    SubmitOK = b"SK"
    CancelOK = b"CK"
    CancelFailed = b"CF"
    Duplicated = b"DC"
    NoWorker = b"NW"
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


class ArgumentType(enum.Enum):
    Data = b"D"
    Task = b"T"


class _Message(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def serialize(self) -> Tuple[bytes, ...]:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize(data: List[bytes]):
        raise NotImplementedError()


@dataclasses.dataclass
class Argument:
    type: ArgumentType
    data: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.type.value, self.data

    @staticmethod
    def deserialize(data: List[bytes]):
        return Argument(ArgumentType(data[0]), data[1])


MessageVariant = TypeVar("MessageVariant", bound=_Message)


@dataclasses.dataclass
class Task(_Message):
    task_id: bytes
    function_id: bytes
    function_args: List[Argument]

    def serialize(self) -> Tuple[bytes, bytes, bytes]:
        return self.task_id, self.function_id, *[d for arg in self.function_args for d in arg.serialize()]

    @staticmethod
    def deserialize(data: List[bytes]):
        return Task(data[0], data[1], [Argument.deserialize(data[i : i + 2]) for i in range(2, len(data), 2)])


@dataclasses.dataclass
class TaskEcho(_Message):
    task_id: bytes
    status: TaskEchoStatus

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskEcho(data[0], TaskEchoStatus(data[1]))


@dataclasses.dataclass
class TaskCancel(_Message):
    task_id: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.task_id,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return TaskCancel(data[0])


@dataclasses.dataclass
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


@dataclasses.dataclass
class GraphTask(_Message):
    task_id: bytes
    functions: Dict[bytes, Tuple[bytes, bytes]]
    targets: List[bytes]
    graph: List[Task]

    def serialize(self) -> Tuple[bytes, ...]:
        function_bytes = list()
        function_bytes.append(struct.pack("I", len(self.functions)))
        function_bytes.extend(self.functions.keys())
        names, content = zip(*self.functions.values())
        function_bytes.extend(names)
        function_bytes.extend(content)

        graph_bytes = []
        for task in self.graph:
            frames = task.serialize()
            graph_bytes.append(struct.pack("I", len(frames)))
            graph_bytes.extend(frames)

        return self.task_id, *function_bytes, struct.pack("I", len(self.targets)), *self.targets, *graph_bytes

    @staticmethod
    def deserialize(data: List[bytes]):
        index = 0
        task_id = data[index]

        index += 1
        number_of_functions = struct.unpack("I", data[index])[0]

        index += 1
        keys = data[index : index + number_of_functions]

        index += number_of_functions
        names = data[index : index + number_of_functions]

        index += number_of_functions
        content = data[index : index + number_of_functions]

        functions = dict(zip(keys, zip(names, content)))

        index += number_of_functions
        number_of_targets = struct.unpack("I", data[index])[0]

        index += 1
        targets = data[index : index + number_of_targets]

        index += number_of_targets
        graph = []
        while index < len(data):
            number_of_frames = struct.unpack("I", data[index])[0]
            index += 1
            graph.append(Task.deserialize(data[index : index + number_of_frames]))
            index += number_of_frames

        return GraphTask(task_id, functions, targets, graph)


@dataclasses.dataclass
class GraphTaskEcho(_Message):
    task_id: bytes
    status: TaskEchoStatus

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value

    @staticmethod
    def deserialize(data: List[bytes]):
        return GraphTaskEcho(data[0], TaskEchoStatus(data[1]))


@dataclasses.dataclass
class GraphTaskCancel(_Message):
    task_id: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.task_id,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return GraphTaskCancel(data[0])


@dataclasses.dataclass
class GraphTaskResult(_Message):
    task_id: bytes
    status: TaskStatus
    results: List[bytes]

    def serialize(self) -> Tuple[bytes, ...]:
        return self.task_id, self.status.value, *self.results

    @staticmethod
    def deserialize(data: List[bytes]):
        return GraphTaskResult(data[0], TaskStatus(data[1]), data[2:])


@dataclasses.dataclass
class BalanceRequest(_Message):
    number_of_tasks: int

    def serialize(self) -> Tuple[bytes, ...]:
        return (struct.pack("I", self.number_of_tasks),)

    @staticmethod
    def deserialize(data: List[bytes]):
        return BalanceRequest(*struct.unpack("I", data[0]))


@dataclasses.dataclass
class BalanceResponse(_Message):
    task_ids: List[bytes]

    def serialize(self) -> Tuple[bytes, ...]:
        return struct.pack("I", len(self.task_ids)), *self.task_ids

    @staticmethod
    def deserialize(data: List[bytes]):
        length = struct.unpack("I", data[0])[0]
        task_ids = list(data[1:])
        assert length == len(task_ids)
        return BalanceResponse(task_ids)


@dataclasses.dataclass
class Heartbeat(_Message):
    agent_cpu: float
    agent_rss: int
    worker_cpu: float
    worker_rss: int
    queued_tasks: int

    def serialize(self) -> Tuple[bytes, ...]:
        return (
            struct.pack("fQfQI", self.agent_cpu, self.agent_rss, self.worker_cpu, self.worker_rss, self.queued_tasks),
        )

    @staticmethod
    def deserialize(data: List[bytes]):
        return Heartbeat(*struct.unpack("fQfQI", data[0]))


@dataclasses.dataclass
class FunctionRequest(_Message):
    type: FunctionRequestType
    function_id: bytes
    function_name: bytes
    content: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.type.value, self.function_id, self.function_name, self.content

    @staticmethod
    def deserialize(data: List[bytes]):
        return FunctionRequest(FunctionRequestType(data[0]), data[1], data[2], data[3])


@dataclasses.dataclass
class FunctionResponse(_Message):
    status: FunctionResponseType
    function_id: bytes
    function_name: bytes
    content: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.status.value, self.function_id, self.function_name, self.content

    @staticmethod
    def deserialize(data: List[bytes]):
        return FunctionResponse(FunctionResponseType(data[0]), data[1], data[2], data[3])


@dataclasses.dataclass
class DisconnectRequest(_Message):
    worker: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.worker,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return DisconnectRequest(data[0])


@dataclasses.dataclass
class DisconnectResponse(_Message):
    worker: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.worker,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return DisconnectResponse(data[0])


@dataclasses.dataclass
class ProcessorInitialize(_Message):
    def serialize(self) -> Tuple[bytes, ...]:
        return (b"",)

    @staticmethod
    def deserialize(data: List[bytes]):
        return ProcessorInitialize()


@dataclasses.dataclass
class SchedulerStatus(_Message):
    data: bytes  # json content represent in bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return (self.data,)

    @staticmethod
    def deserialize(data: List[bytes]):
        return SchedulerStatus(data[0])


PROTOCOL = bidict.bidict(
    {
        MessageType.Heartbeat: Heartbeat,
        MessageType.Task: Task,
        MessageType.TaskEcho: TaskEcho,
        MessageType.TaskCancel: TaskCancel,
        MessageType.TaskResult: TaskResult,
        MessageType.GraphTask: GraphTask,
        MessageType.GraphTaskEcho: GraphTaskEcho,
        MessageType.GraphTaskCancel: GraphTaskCancel,
        MessageType.GraphTaskResult: GraphTaskResult,
        MessageType.BalanceRequest: BalanceRequest,
        MessageType.BalanceResponse: BalanceResponse,
        MessageType.FunctionRequest: FunctionRequest,
        MessageType.FunctionResponse: FunctionResponse,
        MessageType.DisconnectRequest: DisconnectRequest,
        MessageType.DisconnectResponse: DisconnectResponse,
        MessageType.ProcessorInitialize: ProcessorInitialize,
        MessageType.SchedulerStatus: SchedulerStatus,
    }
)
