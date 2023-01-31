import logging
import os
import socket
import threading
from collections import defaultdict
from typing import Awaitable, Callable, List, Literal, Optional

import zmq

from scaled.scheduler.mixins import Connector
from scaled.utility.zmq_config import ZMQConfig
from scaled.protocol.python.message import PROTOCOL, Message
from scaled.protocol.python.objects import MessageType


class AsyncConnector(Connector):
    def __init__(
        self,
        prefix: str,
        address: ZMQConfig,
        stop_event: threading.Event,
        polling_time: int = 1,
    ):
        self._prefix = prefix
        self._address = address

        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.DEALER)
        self._identity: bytes = f"{self._prefix}|{socket.gethostname()}|{os.getpid()}".encode()
        self.__set_socket_options()
        self._socket.connect(self._address.to_address())

        self._polling_time = polling_time

        self._callback: Optional[Callable[[MessageType, Message], Awaitable[None]]] = None
        self._stop_event = stop_event

        self._statistics = {
            "received": defaultdict(lambda: 0),
            "sent": defaultdict(lambda: 0)
        }

    def __del__(self):
        self._socket.close()

    def register(self, callback: Callable[[bytes, MessageType, Message], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        count = self._socket.poll(self._polling_time * 1000)
        if not count:
            return

        for _ in range(count):
            frames = self._socket.recv_multipart()
            if not self.__is_valid_message(frames):
                continue

            message_type_bytes, *payload = frames
            message_type = MessageType(message_type_bytes)
            message = PROTOCOL[message_type_bytes].deserialize(payload)

            self.__count_one("received", message_type)
            self._callback(message_type, message)

    @property
    def identity(self) -> bytes:
        return self._identity

    async def send(self, message_type: MessageType, data: Message):
        self.__count_one("sent", message_type)
        self._socket.send_multipart([message_type.value, *data])

    def statistics(self):
        return self._statistics

    def __set_socket_options(self):
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

    def __count_one(self, count_type: Literal["sent", "received"], message_type: MessageType):
        self._statistics[count_type][message_type.name] += 1

    def __is_valid_message(self, frames: List[bytes]) -> bool:
        if len(frames) < 2:
            logging.error(f"{self.__class__.__name__}: received unexpected frames {frames}")
            return False

        if frames[0] not in {member.value for member in MessageType}:
            logging.error(f"{self._identity}: received unexpected frames {frames}")
            return False

        return True
