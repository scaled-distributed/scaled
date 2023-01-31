import json
import threading
import uuid
from concurrent.futures import Future

from typing import Any, Callable, Dict, Iterable, List, Optional

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.connector import Connector
from scaled.protocol.python.function import FunctionSerializer
from scaled.protocol.python.message import MessageVariant, MonitorRequest, MonitorResponse, Task, TaskEcho, TaskResult
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

        self._futures: Dict[bytes, Future] = dict()

        # debug info
        self._count: int = 0
        self._monitor_future: Optional[Future] = None

    def __del__(self):
        self.disconnect()

    def submit(self, fn: Callable, *args) -> Future:
        task_id = uuid.uuid1().bytes
        task = Task(task_id, FunctionSerializer.serialize_function(fn), FunctionSerializer.serialize_arguments(args))
        self._connector.send(MessageType.Task, task)

        future = Future()
        future.add_done_callback(self.__on_result)
        self._futures[task_id] = future
        return future

    def monitor(self):
        self._monitor_future = Future()
        self._connector.send(MessageType.MonitorRequest, MonitorRequest(b""))
        return self._monitor_future.result()

    def gather(self, futures: Iterable[Future]) -> List[Any]:
        task_ids, results = zip(*[future.result() for future in futures])
        for task_id in task_ids:
            self._futures.pop(task_id)
        return list(results)

    def disconnect(self):
        self._stop_event.set()

    def __on_result(self, result):
        self._count += 1
        print(f"finished: {self._count}, connector: {json.dumps(self._connector.monitor())}")

    def __on_receive(self, message_type: MessageType, data: MessageVariant):
        match message_type:
            case MessageType.TaskEcho:
                self.__on_task_echo(data)
            case MessageType.TaskResult:
                self.__on_task_result(data)
            case MessageType.MonitorResponse:
                self.__on_monitor_response(data)
            case _:
                raise TypeError(f"Unknown {message_type=}")

    def __on_task_echo(self, data: TaskEcho):
        if data.task_id not in self._futures:
            return
        future = self._futures[data.task_id]
        future.set_running_or_notify_cancel()

    def __on_task_result(self, result: TaskResult):
        if result.task_id not in self._futures:
            return
        future = self._futures[result.task_id]
        future.set_result((result.task_id, FunctionSerializer.deserialize_result(result.result)))

    def __on_monitor_response(self, data: MonitorResponse):
        self._monitor_future.set_result(data.data)