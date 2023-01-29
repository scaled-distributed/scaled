import threading
import uuid
from concurrent.futures import Future

from typing import Any, Callable, Dict, Iterable, List, Set

from scaled.io.config import ZMQConfig
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
            callback=self.__on_receive,
            stop_event=self._stop_event,
            polling_time=polling_time,
        )
        self._task_id_to_futures: Dict[bytes, Future] = dict()
        self._ready_futures: Set[Future] = set()
        self._count: int = 0
        self._semaphore: threading.Semaphore = threading.Semaphore(1)

    def __del__(self):
        self.disconnect()

    def submit(self, fn: Callable, *args) -> Future:
        task_id = uuid.uuid1().bytes
        task = Task(task_id, FunctionSerializer.serialize_function(fn), FunctionSerializer.serialize_arguments(args))
        self._connector.send(MessageType.Task, task)

        future = Future()
        self._task_id_to_futures[task_id] = future
        return future

    def gather(self, futures: Iterable[Future]) -> List[Any]:
        results = [future.result() for future in futures]
        self._ready_futures -= set(futures)
        return results

    def disconnect(self):
        self._stop_event.set()

    def __on_receive(self, message_type: MessageType, data: Message):
        match message_type:
            case MessageType.TaskEcho:
                assert isinstance(data, TaskEcho)
                with self._semaphore:
                    self._task_id_to_futures[data.task_id].set_running_or_notify_cancel()
            case MessageType.TaskResult:
                assert isinstance(data, TaskResult)
                with self._semaphore:
                    future = self._task_id_to_futures.pop(data.task_id)
                    future.set_result(FunctionSerializer.deserialize_result(data.result))
                    self._ready_futures.add(future)
            case _:
                raise TypeError(f"Unknown {message_type=}")
