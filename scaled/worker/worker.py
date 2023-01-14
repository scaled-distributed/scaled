import logging
import multiprocessing
import pickle
import threading
from typing import Any, Dict, Optional, Callable

from scaled.io.heartbeat import start_heartbeat
from scaled.io.config import ZMQConfig
from scaled.io.connector import Connector
from scaled.protocol.python.objects import MessageType, TaskStatus
from scaled.protocol.python.message import TaskResult, Task, TaskCancel
from scaled.protocol.python.serializer import Serializer
from scaled.worker.utility import load_function


class Worker(multiprocessing.get_context("spawn").Process):
    def __init__(
        self, address: ZMQConfig, stop_event: multiprocessing.Event, polling_time: int, heartbeat_interval: int
    ):
        multiprocessing.Process.__init__(self, name="worker")

        self._connector: Optional[Connector] = None
        self._address = address
        self._stop_event = stop_event
        self._polling_time = polling_time
        self._heartbeat_interval = heartbeat_interval

        self._heartbeat = None
        self._heartbeat_stop_event = None

        self._cached_functions: Dict[bytes, Any] = {}

    def run(self) -> None:
        self._initialize()
        self._run_forever()

    def _run_forever(self):
        while not self._stop_event.is_set():
            self._on_receive(*self._connector.receive())

        self._heartbeat_stop_event.join()
        print(f"{self._connector.identity}: quiting")

    def _initialize(self):
        self._connector = Connector("W", self._address, self._stop_event, self._polling_time)
        self._heartbeat_stop_event = threading.Event()
        self._heartbeat = start_heartbeat(
            address=self._address,
            worker_identity=self._connector.identity,
            interval=self._heartbeat_interval,
            stop_event=self._heartbeat_stop_event,
        )
        print(f"{self._connector.identity}: started")

    def _on_receive(self, source: bytes, message_type: MessageType, data: Serializer):
        match data:
            case Task():
                self._process_task(data)
            case TaskCancel():
                self._process_task_cancel(data)
            case _:
                logging.exception(f"{self._connector.identity}: unknown {message_type=} {data=} from {source}")

    def _get_function(self, function_name: bytes) -> Callable:
        if function_name in self._cached_functions:
            return self._cached_functions[function_name]

        app = load_function(function_name)
        self._cached_functions[function_name] = app
        return app

    def _process_task(self, task: Task):
        # noinspection PyBroadException
        try:
            function = self._get_function(task.function_name)
            result = pickle.dumps(function(*(pickle.loads(args) for args in task.function_args)))
            self._connector.send(MessageType.TaskResult, TaskResult(task.task_id, TaskStatus.Success, result))
        except Exception as e:
            logging.exception(f"error when processing {task=}:")
            self._connector.send(MessageType.TaskResult, TaskResult(task.task_id, TaskStatus.Failed, str(e).encode()))

    def _process_task_cancel(self, task: Task):
        # TODO: implement this
        pass
