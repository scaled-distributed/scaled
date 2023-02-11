import logging
import os
import socket
from collections import defaultdict
from typing import Awaitable, Callable, List, Literal

import zmq.asyncio

from scaled.io.config import POLLING_TIME_MILLISECONDS
from scaled.utility.zmq_config import ZMQConfig
from scaled.protocol.python.message import MessageType, MessageVariant, PROTOCOL


class AsyncConnector:
    def __init__(
        self,
        prefix: str,
        context: zmq.asyncio.Context,
        socket_type: int,
        address: ZMQConfig,
        bind_or_connect: Literal["bind", "connect"],
        callback: Callable[[MessageType, MessageVariant], Awaitable[None]],
    ):
        self._prefix = prefix
        self._address = address

        self._context = context
        self._socket = self._context.socket(socket_type)
        self._identity: bytes = f"{self._prefix}|{socket.gethostname()}|{os.getpid()}".encode()
        self.__set_socket_options()

        if bind_or_connect == "bind":
            self._socket.bind(self._address.to_address())
        elif bind_or_connect == "connect":
            self._socket.connect(self._address.to_address())
        else:
            raise TypeError(f"bind_or_connect has to be 'bind' or 'connect'")

        self._callback: Callable[[MessageType, MessageVariant], Awaitable[None]] = callback

        self._statistics = {"received": defaultdict(lambda: 0), "sent": defaultdict(lambda: 0)}

    def __del__(self):
        self._socket.close()

    @property
    def identity(self) -> bytes:
        return self._identity

    async def routine(self):
        count = await self._socket.poll(POLLING_TIME_MILLISECONDS)
        if not count:
            return

        for _ in range(count):
            frames = await self._socket.recv_multipart()
            if not self.__is_valid_message(frames):
                continue

            message_type_bytes, *payload = frames
            message_type = MessageType(message_type_bytes)
            message = PROTOCOL[message_type_bytes].deserialize(payload)

            self.__count_one("received", message_type)
            await self._callback(message_type, message)

    async def send(self, message_type: MessageType, data: MessageVariant):
        self.__count_one("sent", message_type)
        await self._socket.send_multipart([message_type.value, *data.serialize()])

    async def statistics(self):
        return self._statistics

    def __set_socket_options(self):
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

    def __count_one(self, count_type: Literal["sent", "received"], message_type: MessageType):
        self._statistics[count_type][message_type.name] += 1

    def __is_valid_message(self, frames: List[bytes]) -> bool:
        if len(frames) < 2:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return False

        if frames[0] not in {member.value for member in MessageType}:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return False

        return True

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
