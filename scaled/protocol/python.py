import abc
import pickle
import struct
from typing import Tuple, List, Dict

import attrs

from scaled.io.objects import MessageType, TaskStatus


class Serializer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def serialize(self) -> Tuple[bytes, ...]:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize(data: List[bytes]):
        raise NotImplementedError()


@attrs.define
class ClientMapJob(Serializer):
    job_id: int
    function_name: bytes
    function: bytes
    list_of_args: List[bytes]

    def serialize(self) -> Tuple[bytes, ...]:
        return struct.pack("l", self.job_id), self.function_name, self.function, *self.list_of_args

    @staticmethod
    def deserialize(data: List[bytes]):
        return ClientMapJob(struct.unpack("l", data[0]), data[1], data[2], *data[2:])


@attrs.define
class ClientGraphJob(Serializer):
    job_id: int
    graph_payload: Dict

    def serialize(self) -> Tuple[bytes, ...]:
        return pickle.dumps(self.graph_payload),

    @staticmethod
    def deserialize(data: List[bytes]):
        return ClientGraphJob(pickle.loads(data[0]))


@attrs.define
class ClientJobResult(Serializer):
    job_id: int
    results: Tuple[bytes, ...]

    def serialize(self) -> Tuple[bytes, ...]:
        return struct.pack("l", self.job_id), *self.results

    @staticmethod
    def deserialize(data: List[bytes]):
        return ClientJobResult(struct.unpack("l", data[0]), tuple(data[1:]))


@attrs.define
class ClientJobEcho(Serializer):
    job_id: int

    def serialize(self) -> Tuple[bytes, ...]:
        return struct.pack("l", self.job_id),

    @staticmethod
    def deserialize(data: List[bytes]):
        return ClientJobEcho(struct.unpack("l", data[0])[0])


@attrs.define
class FunctionRequest(Serializer):
    job_id: int
    function_name: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return struct.pack("l", self.task_id), self.function_name

    @staticmethod
    def deserialize(data: List[bytes]):
        return FunctionRequest(struct.unpack("l", data[0])[0], data[1])


@attrs.define
class AddFunction(Serializer):
    function_name: bytes
    function: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.function_name, self.function

    @staticmethod
    def deserialize(data: List[bytes]):
        return AddFunction(*data)


@attrs.define
class DelFunction(Serializer):
    function_name: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.function_name,

    @staticmethod
    def deserialize(data: List[bytes]):
        return DelFunction(*data)


@attrs.define
class WorkerTask(Serializer):
    task_id: int
    source: bytes
    function_name: bytes
    function_args: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return struct.pack("l", self.task_id), self.source, self.function_name, self.function_args

    @staticmethod
    def deserialize(data: List[bytes]):
        return WorkerTask(struct.unpack("l", data[0])[0], *data[1:])


@attrs.define
class WorkerTaskResult(Serializer):
    task_id: int
    status: TaskStatus
    task_result: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return struct.pack("l", self.task_id), self.status.value, self.task_result

    @staticmethod
    def deserialize(data: List[bytes]):
        return WorkerTaskResult(struct.unpack("l", data[0])[0], TaskStatus(data[1]), data[2])


@attrs.define
class Heartbeat(Serializer):
    byte_data: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return struct.pack("f", self.byte_data),

    @staticmethod
    def deserialize(data: List[bytes]):
        return Heartbeat(struct.unpack("f", data[0]))


PROTOCOL = {
    MessageType.WorkerHeartbeat.value: Heartbeat,
    MessageType.ClientJobMap.value: ClientMapJob,
    MessageType.ClientJobGraph.value: ClientGraphJob,
    MessageType.ClientJobEcho.value: ClientJobEcho,
    MessageType.ClientJobResult.value: ClientJobResult,
    MessageType.WorkerFunctionRequest.value: FunctionRequest,
    MessageType.WorkerFunctionAdd.value: AddFunction,
    MessageType.WorkerFunctionDelete.value: DelFunction,
    MessageType.WorkerTask.value: WorkerTask,
    MessageType.WorkerTaskResult.value: WorkerTaskResult,
}
