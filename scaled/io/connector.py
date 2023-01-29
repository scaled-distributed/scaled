import logging
import os
import socket
import threading
from typing import Callable, List

import zmq

from scaled.io.config import ZMQConfig
from scaled.protocol.python.message import PROTOCOL, Message
from scaled.protocol.python.objects import MessageType


class Connector(threading.Thread):
    def __init__(
        self,
        prefix: str,
        address: ZMQConfig,
        callback: Callable[[MessageType, Message], None],
        stop_event: threading.Event,
        polling_time: int = 1,
    ):
        threading.Thread.__init__(self)

        self._prefix = prefix
        self._address = address

        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.DEALER)
        self._identity: bytes = f"{self._prefix}|{socket.gethostname()}|{os.getpid()}".encode()

        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.connect(self._address.to_address())

        self._polling_time = polling_time

        self._callback = callback
        self._stop_event = stop_event

        self.start()

    def __del__(self):
        self._socket.close()

    @property
    def identity(self) -> bytes:
        return self._identity

    def run(self) -> None:
        while not self._stop_event.is_set():
            while count := self._socket.poll(self._polling_time * 1000):
                for i in range(count):
                    frames = self._socket.recv_multipart()
                    self._on_receive(frames)

    def send(self, message_type: MessageType, data: Message):
        self._socket.send_multipart([message_type.value, *data.serialize()])

    def _on_receive(self, frames: List[bytes]):
        if len(frames) < 3:
            logging.error(f"{self._identity}: received unexpected frames {frames}")
            return

        message_type, *payload = frames
        self._callback(MessageType(message_type), PROTOCOL[message_type].deserialize(payload))
