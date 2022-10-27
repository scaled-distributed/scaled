import enum
import struct
from collections import namedtuple
from typing import List, NamedTuple

import attrs


class JobType(enum.Enum):
    Graph = b"G"
    Map = b"M"


@attrs.define
class Task:
    task_id: int
    source: bytes
    function_name: bytes
    args: bytes


class MessageType(enum.Enum):
    HeartbeatInfo = b"IF"
    SubmitTask = b"ST"
    FunctionRequest = b"FR"
    FunctionAddInstruction = b"AF"
    FunctionDeleteInstruction = b"DF"
    Task = b"TK"
    TaskResult = b"RT"


@attrs.define
class HeartbeatInfo:
    cpu_percent: float

    def to_bytes(self) -> bytes:
        return struct.pack("f", self.cpu_percent)

    @staticmethod
    def from_bytes(data: bytes) -> "HeartbeatInfo":
        return HeartbeatInfo(*struct.unpack("f", data))
