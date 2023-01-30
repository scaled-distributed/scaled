import threading
import uuid
from concurrent.futures import Future

from typing import Any, Callable, Dict, Iterable, List, Set

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.connector import Connector
from scaled.protocol.python.function import FunctionSerializer
from scaled.protocol.python.message import MessageVariant, Task, TaskEcho, TaskResult
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
        self._running_futures: Dict[bytes, Future] = dict()
        self._finished_futures: Set[Future] = set()

        self._count: int = 0
        self._semaphore: threading.Semaphore = threading.Semaphore(1)

    def __del__(self):
        self.disconnect()

    def submit(self, fn: Callable, *args) -> Future:
        task_id = uuid.uuid1().bytes
        task = Task(task_id, FunctionSerializer.serialize_function(fn), FunctionSerializer.serialize_arguments(args))
        self._connector.send(MessageType.Task, task)

        future = Future()
        future.add_done_callback(self.__on_result)
        with self._semaphore:
            self._running_futures[task_id] = future
        return future

    def gather(self, futures: Iterable[Future]) -> List[Any]:
        results = [future.result() for future in futures]
        with self._semaphore:
            self._finished_futures -= set(futures)
        return results

    def disconnect(self):
        self._stop_event.set()

    def __on_result(self, result):
        self._count += 1
        if self._count % 1000 == 0:
            print(f"running: {len(self._running_futures)} finished: {len(self._finished_futures)}")

    def __on_receive(self, message_type: MessageType, data: MessageVariant):
        match message_type:
            case MessageType.TaskEcho:
                self.__on_task_echo(data)
            case MessageType.TaskResult:
                self.__on_task_result(data)
            case _:
                raise TypeError(f"Unknown {message_type=}")

    def __on_task_echo(self, data: TaskEcho):
        with self._semaphore:
            if data.task_id not in self._running_futures:
                return

            future = self._running_futures[data.task_id]
            future.set_running_or_notify_cancel()

    def __on_task_result(self, result: TaskResult):
        with self._semaphore:
            if result.task_id not in self._running_futures:
                return

            future = self._running_futures.pop(result.task_id)
            future.set_result(FunctionSerializer.deserialize_result(result.result))
            self._finished_futures.add(future)
