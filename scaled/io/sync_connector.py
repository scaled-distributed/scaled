import copy
import logging
import os
import socket
import threading
from collections import defaultdict
from queue import Queue
from typing import Callable, Literal, Optional

import zmq

from scaled.utility.zmq_config import ZMQConfig
from scaled.protocol.python.message import PROTOCOL, Message
from scaled.protocol.python.objects import MessageType


class SyncConnector(threading.Thread):
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

        self._context: Optional[zmq.Context] = None
        self._socket: Optional[zmq.Socket] = None
        self._identity: Optional[bytes] = None

        self._polling_time = polling_time

        self._callback = callback
        self._stop_event = stop_event

        self._send_queue = Queue()

        self._mutex = threading.Lock()
        self._statistics = {
            "received": defaultdict(lambda: 0),
            "sent": defaultdict(lambda: 0)
        }

        self.start()

    def __del__(self):
        self._socket.close()

    def ready(self) -> bool:
        return self._identity is not None

    @property
    def identity(self) -> bytes:
        return self._identity

    def run(self) -> None:
        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.DEALER)
        self._identity: bytes = f"{self._prefix}|{socket.gethostname()}|{os.getpid()}".encode()
        self.__set_socket_options()
        self._socket.connect(self._address.to_address())

        while not self._stop_event.is_set():
            self.__send_routine()
            self.__receive_routine()

    def send(self, message_type: MessageType, data: Message):
        self._send_queue.put((message_type, data.serialize()))

    def monitor(self):
        with self._mutex:
            return copy.copy(self._statistics)

    def __set_socket_options(self):
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

    def __send_routine(self):
        while not self._send_queue.empty():
            message_type, data = self._send_queue.get()
            self.__count_one("sent", message_type)
            self._socket.send_multipart([message_type.value, *data])

    def __receive_routine(self):
        count = self._socket.poll(self._polling_time * 1000)
        if not count:
            return

        for _ in range(count):
            frames = self._socket.recv_multipart()
            if len(frames) < 2:
                logging.error(f"{self._identity}: received unexpected frames {frames}")
                return

            if frames[0] not in {member.value for member in MessageType}:
                logging.error(f"{self._identity}: received unexpected frames {frames}")
                return

            message_type_bytes, *payload = frames
            message_type = MessageType(message_type_bytes)
            message = PROTOCOL[message_type_bytes].deserialize(payload)

            self.__count_one("received", message_type)
            self._callback(message_type, message)

    def __count_one(self, count_type: Literal["sent", "received"], message_type: MessageType):
        with self._mutex:
            self._statistics[count_type][message_type.name] += 1