import logging
import os
import socket
import uuid
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Literal, Optional

import zmq.asyncio

from scaled.scheduler.mixins import Looper, Reporter
from scaled.utility.zmq_config import ZMQConfig
from scaled.protocol.python.message import MessageType, MessageVariant, PROTOCOL


class AsyncBinder(Looper, Reporter):
    def __init__(self, prefix: str, address: ZMQConfig, io_threads: int):
        self._address = address
        self._identity: bytes = f"{prefix}|{socket.gethostname().split('.')[0]}|{os.getpid()}|{uuid.uuid4()}".encode()

        self._context = zmq.asyncio.Context(io_threads=io_threads)
        self._socket = self._context.socket(zmq.ROUTER)
        self.__set_socket_options()
        self._socket.bind(self._address.to_address())

        self._callback: Optional[Callable[[bytes, MessageType, MessageVariant], Awaitable[None]]] = None

        self._statistics = {"received": defaultdict(lambda: 0), "sent": defaultdict(lambda: 0)}

    def destroy(self):
        self._context.destroy(linger=0)

    def register(self, callback: Callable[[bytes, MessageType, MessageVariant], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        frames = await self._socket.recv_multipart()
        if not self.__is_valid_message(frames):
            return

        source, message_type_bytes, payload = frames[0], frames[1], frames[2:]
        message_type = MessageType(message_type_bytes)
        self.__count_one("received", message_type)
        message = PROTOCOL[message_type_bytes].deserialize(payload)
        await self._callback(source, message_type, message)

    async def statistics(self) -> Dict:
        return {
            "binder": {
                "received": {k: v for k, v in self._statistics["received"].items()},
                "sent": {k: v for k, v in self._statistics["sent"].items()},
            }
        }

    async def send(self, to: bytes, message_type: MessageType, message: MessageVariant):
        self.__count_one("sent", message_type)
        await self._socket.send_multipart([to, message_type.value, *message.serialize()], copy=False)

    def __set_socket_options(self):
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

    def __is_valid_message(self, frames: List[bytes]) -> bool:
        if len(frames) < 3:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return False

        if frames[1] not in {member.value for member in MessageType}:
            logging.error(f"{self.__get_prefix()} received unexpected message type: {frames[0]}")
            return False

        return True

    def __count_one(self, count_type: Literal["sent", "received"], message_type: MessageType):
        self._statistics[count_type][message_type.name] += 1

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
