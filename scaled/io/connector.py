import logging
import multiprocessing
import os
import socket
from typing import Optional, Tuple

import zmq

from scaled.io.config import ZMQConfig
from scaled.protocol.python.message import PROTOCOL
from scaled.protocol.python.objects import MessageType

from scaled.protocol.python.serializer import Serializer


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

    def receive(self) -> Tuple[bytes, MessageType, Serializer]:
        while self._stop_event is None or not self._stop_event.is_set():
            socks = self._poller.poll(self._polling_time)
            if socks:
                break

        frames = self._socket.recv_multipart()
        if len(frames) < 3:
            logging.error(f"{self._identity}: received unexpected frames {frames}")

        source, message_type, *payload = frames
        return source, MessageType(message_type), PROTOCOL[message_type].deserialize(payload)

    def send(self, message_type: MessageType, data: Serializer):
        self._socket.send_multipart([message_type.value, *data.serialize()])
