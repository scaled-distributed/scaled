import threading
import logging
import os
import socket
from typing import Awaitable, Callable, Optional

import zmq.asyncio

from scaled.io.config import ZMQConfig
from scaled.protocol.python.message import Message, PROTOCOL
from scaled.protocol.python.objects import MessageType
from scaled.router.mixins import Binder

POLLING_TIME = 1000


class AsyncBinder(Binder):
    def __init__(self, stop_event: threading.Event, prefix: str, address: ZMQConfig):
        self._address = address
        self._identity: bytes = f"{prefix}|{socket.gethostname()}|{os.getpid()}".encode()

        self._context = zmq.asyncio.Context.instance()
        self._socket = self._context.socket(zmq.ROUTER)
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.bind(self._address.to_address())

        self._stop_event = stop_event
        logging.info(f"{self.__class__.__name__}: bind to {address.to_address()}")

        self._callback: Optional[Callable[[bytes, MessageType, Message], Awaitable[None]]] = None

    def register(self, callback: Callable[[bytes, MessageType, Message], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        count = await self._socket.poll(POLLING_TIME)
        if not count:
            return

        for _ in range(count):
            frames = await self._socket.recv_multipart()
            if len(frames) < 3:
                logging.error(f"{self.__class__.__name__}: received unexpected frames {frames}")
                continue

            source, message_type_bytes, payload = frames[0], frames[1], frames[2:]
            message_type = MessageType(message_type_bytes)
            message = PROTOCOL[message_type_bytes].deserialize(payload)
            await self._callback(source, message_type, message)

    async def send(self, to: bytes, message_type: MessageType, data: Message):
        await self._socket.send_multipart([to, message_type.value, *data.serialize()])
