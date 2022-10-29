import multiprocessing
import os
import socket
from typing import List, Optional

import zmq

from scaled.io.config import ZMQConfig
from scaled.io.objects import MessageType

from scaled.protocol.python import Serializer


class Connector:
    def __init__(
        self,
        prefix: str,
        address: ZMQConfig,
        stop_event: Optional[multiprocessing.Event] = None,
        polling_time: int = 1000,
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

    def __del__(self):
        self._socket.close()

    @property
    def identity(self) -> bytes:
        return self._identity

    def receive(self) -> List[bytes]:
        while self._stop_event is None or not self._stop_event.is_set():
            socks = self._poller.poll(self._polling_time)
            if socks:
                break

        return self._socket.recv_multipart()

    def send(self, message_type: MessageType, data: Serializer):
        self._socket.send_multipart([message_type.value, *data.serialize()])
