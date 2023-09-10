import copy
import logging
import os
import socket
import threading
import uuid
from collections import defaultdict
from queue import Queue
from typing import Callable
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional

import zmq

from scaled.io.config import POLLING_TIME_MILLISECONDS
from scaled.protocol.python.message import MessageType
from scaled.protocol.python.message import MessageVariant
from scaled.protocol.python.message import PROTOCOL
from scaled.utility.zmq_config import ZMQConfig


class SyncConnector(threading.Thread):
    def __init__(
        self,
        stop_event: threading.Event,
        context: zmq.Context,
        socket_type: int,
        bind_or_connect: Literal["bind", "connect"],
        address: ZMQConfig,
        callback: Callable[[MessageVariant], None],
        exit_callback: Optional[Callable[[], None]],
        daemonic: bool,
    ):
        threading.Thread.__init__(self)
        self._address = address

        self._context = context
        self._socket = self._context.socket(socket_type)
        self._identity: bytes = f"{os.getpid()}|{socket.gethostname().split('.')[0]}|{uuid.uuid4()}".encode()

        # set socket option
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

        if daemonic:
            self.daemon = True

        if bind_or_connect == "bind":
            self._socket.bind(self._address.to_address())
        elif bind_or_connect == "connect":
            self._socket.connect(self._address.to_address())
        else:
            raise TypeError("bind_or_connect has to be 'bind' or 'connect'")

        self._callback = callback
        self._exit_callback = exit_callback
        self._stop_event = stop_event

        self._send_queue: Queue = Queue()

        self._statistics_mutex = threading.Lock()
        self._statistics: Dict[str, Dict[str, int]] = {
            "received": defaultdict(lambda: 0),
            "sent": defaultdict(lambda: 0),
        }

    def close(self):
        self._socket.close()

    @property
    def identity(self) -> bytes:
        return self._identity

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.__routine_send()
            self.__routine_polling()

        if self._exit_callback is not None:
            self._exit_callback()

        self.close()

    def send(self, message: MessageVariant):
        self._send_queue.put(message)

    def send_immediately(self, message: MessageVariant):
        message_type = PROTOCOL.inverse[type(message)]
        self._socket.send_multipart([message_type.value, *message.serialize()], copy=False)

    def monitor(self):
        with self._statistics_mutex:
            return copy.copy(self._statistics)

    def __routine_send(self):
        while not self._send_queue.empty():
            message = self._send_queue.get()
            message_type = PROTOCOL.inverse[type(message)]
            self._socket.send_multipart([message_type.value, *message.serialize()])

    def __routine_polling(self):
        try:
            count = self._socket.poll(POLLING_TIME_MILLISECONDS)

            if not count:
                return

            for _ in range(count):
                frames = self._socket.recv_multipart()
                self.__routine_receive(frames)

        except zmq.ZMQError:
            return

    def __routine_receive(self, frames: List[bytes]):
        if len(frames) < 2:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return

        if frames[0] not in {member.value for member in MessageType}:
            logging.error(f"{self.__get_prefix()} received unexpected message type: {frames[0]!r}")
            return

        message_type_bytes, *payload = frames
        message_type = MessageType(message_type_bytes)
        message = PROTOCOL[message_type].deserialize(payload)

        self.__count_one("received", message_type)
        self._callback(message)

    def __count_one(self, count_type: Literal["sent", "received"], message_type: MessageType):
        with self._statistics_mutex:
            self._statistics[count_type][message_type.name] += 1

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
