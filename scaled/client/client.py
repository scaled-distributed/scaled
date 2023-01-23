import threading
import uuid
from concurrent.futures import Future

from typing import Any, Callable, Dict, Iterable, List

from scaled.io.config import ZMQConfig, ZMQType
from scaled.io.connector import Connector
from scaled.protocol.python.function import FunctionSerializer
from scaled.protocol.python.message import Message, Task, TaskEcho, TaskResult
from scaled.protocol.python.objects import MessageType


class Client:
    def __init__(self, config: ZMQConfig, polling_time: int = 1):
        self._stop_event = threading.Event()
        self._connector = Connector(
            prefix="C",
            address=config,
            callback=self._on_receive,
            stop_event=self._stop_event,
            polling_time=polling_time,
        )
        self._futures: Dict[bytes, Future] = dict()

    def __del__(self):
        self._stop_event.set()

    def submit(self, fn: Callable, *args) -> Future:
        task_id = uuid.uuid1().bytes
        task = Task(task_id, *FunctionSerializer.serialize_function(fn, args))
        self._connector.send(MessageType.Task, task)

        future = Future()
        self._futures[task_id] = future
        return future

    def gather(self, futures: Iterable[Future]) -> List[Any]:
        for future in futures:
            if future not in self._futures:
                raise ValueError(f"future not recognized: {future}")

        return [future.result() for future in futures]

    def _on_receive(self, message_type: MessageType, data: Message):
        match message_type:
            case MessageType.TaskEcho:
                assert isinstance(data, TaskEcho)
                self._futures[data.task_id].set_running_or_notify_cancel()
            case MessageType.TaskResult:
                assert isinstance(data, TaskResult)
                self._futures[data.task_id].set_result(FunctionSerializer.deserialize_result(data.result))
            case _:
                raise TypeError(f"Unknown {message_type=}")
