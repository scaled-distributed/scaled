import logging
import asyncio
import os
import socket
from typing import AsyncGenerator
import zmq.asyncio

from scaled.utility.zmq_config import ZMQConfig
from scaled.protocol.python.message import PROTOCOL, Message
from scaled.protocol.python.objects import MessageType


class AsyncConnector:
    def __init__(self, prefix: str, address: ZMQConfig, stop_event: asyncio.Event, polling_time: int = 1000):
        self._context = zmq.asyncio.Context.instance()

        self._address = address
        self._socket = self._context.socket(zmq.DEALER)
        self._identity: bytes = f"{prefix}|{socket.gethostname()}|{os.getpid()}".encode()

        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.connect(address.to_address())
        self._polling_time = polling_time

        self._stop_event = stop_event

    def __del__(self):
        self._socket.close()

    @property
    def identity(self) -> bytes:
        return self._identity

    async def receive(self) -> AsyncGenerator[bytes, MessageType, Message]:
        while self._stop_event is None or not self._stop_event.is_set():
            while count := await self._socket.poll(self._polling_time):
                for _ in range(count):
                    frames = await self._socket.recv_multipart()
                    if len(frames) < 3:
                        logging.error(f"{self._identity}: received unexpected frames {frames}")

                    source, message_type, *payload = frames
                    yield source, MessageType(message_type), PROTOCOL[message_type].deserialize(payload)

    async def send(self, message_type: MessageType, data: Message):
        await self._socket.send_multipart([message_type.value, *data.serialize()])
