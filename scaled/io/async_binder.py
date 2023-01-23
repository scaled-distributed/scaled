import threading
import logging
import os
import socket
from typing import Awaitable, Callable, List, Optional

import zmq.asyncio

from scaled.io.config import ZMQConfig
from scaled.protocol.python.message import Message, PROTOCOL
from scaled.protocol.python.objects import MessageType
from scaled.router.mixins import Binder


class AsyncBinder(Binder):
    def __init__(self, stop_event: threading.Event, prefix: str, address: ZMQConfig, polling_time: int = 1000):
        self._address = address
        self._context = zmq.asyncio.Context.instance()
        self._socket = self._context.socket(zmq.ROUTER)
        self._identity: bytes = f"{prefix}|{socket.gethostname()}|{os.getpid()}".encode()

        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.bind(address.to_address())

        self._poller = zmq.asyncio.Poller()
        self._poller.register(self._socket, zmq.POLLIN)
        self._polling_time = polling_time

        self._stop_event = stop_event

        self._callback: Optional[Callable[[bytes, MessageType, Message], Awaitable[None]]] = None

    def register(self, callback: Callable[[bytes, MessageType, Message], Awaitable[None]]):
        self._callback = callback

    async def loop(self):
        if self._callback is None:
            raise ValueError(f"please use ZMQBinder.register() to register callback before start")

        while not self._stop_event.is_set():
            for sock, msg in await self._poller.poll(self._polling_time):
                frames = await sock.recv_multipart()
                if len(frames) < 3:
                    logging.error(f"{self._identity}: received unexpected frames {frames}")
                    continue

                source, message_type_bytes, payload = frames[0], frames[1], frames[2:]
                message_type = MessageType(message_type_bytes)
                message = PROTOCOL[message_type_bytes].deserialize(payload)
                await self._callback(source, message_type, message)

    async def send(self, to: bytes, message_type: MessageType, data: Message):
        await self._socket.send_multipart([to, message_type.value, *data.serialize()])
