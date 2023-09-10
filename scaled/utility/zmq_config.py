import dataclasses
import enum
from typing import Optional


class ZMQType(enum.Enum):
    inproc = "inproc"
    ipc = "ipc"
    tcp = "tcp"

    @staticmethod
    def allowed_types():
        return {t.value for t in ZMQType}


@dataclasses.dataclass
class ZMQConfig:
    type: ZMQType
    host: str
    port: Optional[int] = None

    def __post_init__(self):
        if not isinstance(self.type, ZMQType):
            raise TypeError(f"Invalid zmq type {self.type}, available types are: {ZMQType.allowed_types()}")

        if not isinstance(self.host, str):
            raise TypeError(f"Host should be string, given {self.host}")

        if self.port is None:
            if self.type == ZMQType.tcp:
                raise ValueError(f"type {self.type.value} should have `port`")
        else:
            if self.type in {ZMQType.inproc, ZMQType.ipc}:
                raise ValueError(f"type {self.type.value} should not have `port`")

            if not isinstance(self.port, int):
                raise TypeError(f"Port should be integer, given {self.port}")

    def to_address(self):
        if self.type == ZMQType.tcp:
            return f"tcp://{self.host}:{self.port}"

        if self.type in {ZMQType.inproc, ZMQType.ipc}:
            return f"{self.type.value}://{self.host}"

        raise TypeError(f"Unsupported ZMQ type: {self.type}")

    @staticmethod
    def from_string(string: str) -> "ZMQConfig":
        if "://" not in string:
            raise ValueError("valid ZMQ config should be like tcp://127.0.0.1:12345")

        socket_type_str, host_port = string.split("://", 1)
        if socket_type_str not in ZMQType.allowed_types():
            raise ValueError(f"supported ZMQ types are: {ZMQType.allowed_types()}")

        socket_type = ZMQType(socket_type_str)
        if socket_type in {ZMQType.inproc, ZMQType.ipc}:
            host = host_port
            port = None
        else:
            host, port_str = host_port.split(":")
            try:
                port = int(port_str)
            except ValueError:
                raise ValueError(f"cannot convert '{port_str}' to port number")

        return ZMQConfig(ZMQType(socket_type), host, port)
