import os
import socket
from typing import List, Callable, Awaitable, Optional

import asyncio

import zmq
import zmq.asyncio

from scaled.system.config import ZMQConfig


class Binder:
    def __init__(
        self,
        prefix: str,
        address: ZMQConfig,
        stop_event: asyncio.Event,
        polling_time: int = 1000
    ):
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

        self._callback: Optional[Callable[[List[bytes]], Awaitable[None]]] = None

    def register(self, callback: Callable[[List[bytes]], Awaitable[None]]):
        self._callback = callback

    async def start(self):
        if self._callback is None:
            raise ValueError(f"please use Driver.register() to register callback before start")

        while not self._stop_event.is_set():
            for sock, msg in await self._poller.poll(self._polling_time):
                frames = await sock.recv_multipart()
                await self._callback(frames)

    async def send(self, messages: List[bytes]):
        await self._socket.send_multipart(messages)
