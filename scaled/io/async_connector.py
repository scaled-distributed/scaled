import logging
import os
import socket
import uuid
from collections import defaultdict
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional

import zmq.asyncio

from scaled.protocol.python.message import MessageType
from scaled.protocol.python.message import MessageVariant
from scaled.protocol.python.message import PROTOCOL
from scaled.utility.zmq_config import ZMQConfig


class AsyncConnector:
    def __init__(
        self,
        context: zmq.asyncio.Context,
        socket_type: int,
        address: ZMQConfig,
        bind_or_connect: Literal["bind", "connect"],
        callback: Optional[Callable[[MessageVariant], Coroutine[Any, Any, Any]]],
    ):
        self._address = address

        self._context = context
        self._socket = self._context.socket(socket_type)
        self._identity: bytes = (
            f"{os.getpid()}|{socket.gethostname().split('.')[0]}|{uuid.uuid4().bytes.hex()}".encode()
        )

        # set socket option
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

        if bind_or_connect == "bind":
            self._socket.bind(self._address.to_address())
        elif bind_or_connect == "connect":
            self._socket.connect(self._address.to_address())
        else:
            raise TypeError("bind_or_connect has to be 'bind' or 'connect'")

        self._callback: Optional[Callable[[MessageVariant], Coroutine[Any, Any, Any]]] = callback

        self._statistics: Dict[str, Dict[str, int]] = {
            "received": defaultdict(lambda: 0),
            "sent": defaultdict(lambda: 0),
        }

    def __del__(self):
        self.destroy()

    def destroy(self):
        self._context.destroy(linger=1)

    @property
    def identity(self) -> bytes:
        return self._identity

    async def routine(self):
        frames = await self._socket.recv_multipart()
        if not self.__is_valid_message(frames):
            return

        message_type_bytes, *payload = frames
        message_type = MessageType(message_type_bytes)
        message = PROTOCOL[message_type].deserialize(payload)

        self.__count_one("received", message_type)
        if self._callback is None:
            logging.error(f"{self.__get_prefix()} received message but didn't set callback")
            return

        await self._callback(message)

    async def send(self, data: MessageVariant):
        message_type = PROTOCOL.inverse[type(data)]
        self.__count_one("sent", message_type)
        await self._socket.send_multipart([message_type.value, *data.serialize()], copy=False)

    async def statistics(self):
        return self._statistics

    def __count_one(self, count_type: Literal["sent", "received"], message_type: MessageType):
        self._statistics[count_type][message_type.name] += 1

    def __is_valid_message(self, frames: List[bytes]) -> bool:
        if len(frames) < 2:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return False

        if frames[0] not in {member.value for member in MessageType}:
            logging.error(f"{self.__get_prefix()} received unexpected message type: {frames[0]!r}")
            return False

        return True

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
