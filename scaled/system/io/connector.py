import multiprocessing
import os
import socket
from typing import List, Optional

import zmq

from scaled.system.config import ZMQConfig


class Connector:
    def __init__(
        self,
        prefix: str,
        address: ZMQConfig,
        stop_event: Optional[multiprocessing.Event] = None,
        polling_time: int = 1000
    ):
        self._address = address
        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.DEALER)
        self._identity: bytes = f"{prefix}|{socket.gethostname()}|{os.getpid()}".encode()

        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.connect(address.to_address())

        self._poller = zmq.Poller()
        self._poller.register(self._socket, zmq.POLLIN)
        self._polling_time = polling_time

        self._stop_event = stop_event

    @property
    def identity(self) -> bytes:
        return self._identity

    def receive(self) -> List[bytes]:
        while self._stop_event is None or not self._stop_event.is_set():
            socks = self._poller.poll(self._polling_time)
            if socks:
                break

        return self._socket.recv_multipart()

    def send(self, messages: List[bytes]):
        self._socket.send_multipart(messages)
