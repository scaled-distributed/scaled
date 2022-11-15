import pickle
import struct
from typing import Tuple, List, Dict

import attrs

from scaled.protocol.python.objects import MessageType, JobStatus
from scaled.protocol.python.serializer import Serializer


@attrs.define
class Job(Serializer):
    job_id: bytes
    tree: Dict[bytes, "Job"]
    function_name: bytes
    list_of_args: Tuple[bytes]

    def serialize(self) -> Tuple[bytes, ...]:
        return self.job_id, self.function_name, *self.list_of_args

    @staticmethod
    def deserialize(data: List[bytes]):
        return Job(data[0], data[1], tuple(data[2:]))


@attrs.define
class JobResult(Serializer):
    job_id: bytes
    function_name: bytes
    status: JobStatus
    results: Tuple[bytes, ...]

    def serialize(self) -> Tuple[bytes, ...]:
        return self.job_id, self.status.value, *self.results

    @staticmethod
    def deserialize(data: List[bytes]):
        return JobResult(data[0], JobStatus(data[1]), tuple(data[2:]))


@attrs.define
class JobEcho(Serializer):
    job_id: bytes

    def serialize(self) -> Tuple[bytes, ...]:
        return self.job_id,

    @staticmethod
    def deserialize(data: List[bytes]):
        return JobEcho(struct.unpack("l", data[0])[0])


@attrs.define
class Heartbeat(Serializer):
    identity: bytes
    cpu_usage: float

    def serialize(self) -> Tuple[bytes, ...]:
        return self.identity, struct.pack("f", self.cpu_usage),

    @staticmethod
    def deserialize(data: List[bytes]):
        return Heartbeat(data[0], struct.unpack("f", data[1])[0])


PROTOCOL = {
    MessageType.Heartbeat.value: Heartbeat,
    MessageType.Job.value: Job,
    MessageType.JobEcho.value: JobEcho,
    MessageType.JobResult.value: JobResult,
}
