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
    function: bytes
    list_of_args: List[bytes]


class MessageType(enum.Enum):
    Info = b"IF"
    AddFunction = b"AF"
    DelFunction = b"DF"
    Task = b"TK"
    Result = b"RT"


@attrs.define
class HeartbeatInfo:
    cpu_percent: float

    def to_bytes(self) -> bytes:
        return struct.pack("f", self.cpu_percent)

    @staticmethod
    def from_bytes(data: bytes) -> "HeartbeatInfo":
        return HeartbeatInfo(*struct.unpack("f", data))


UnitTask = namedtuple("UnitTask", ["id", "function_name", "args"])
UnitResult = namedtuple("UnitResult", ["id", "result"])
