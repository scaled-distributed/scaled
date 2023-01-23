import logging
import multiprocessing
import pickle
import threading
import time
from typing import Any, Callable, Dict, Optional

from scaled.io.config import ZMQConfig
from scaled.io.connector import Connector
from scaled.protocol.python.message import Message, Task, TaskCancel, TaskResult
from scaled.protocol.python.objects import MessageType, TaskStatus
from scaled.utility.logging import setup_logger
from scaled.worker.heartbeat import WorkerHeartbeat
from scaled.protocol.python.function import load_function


class Worker(multiprocessing.get_context("spawn").Process):
    def __init__(
        self, address: ZMQConfig, stop_event: multiprocessing.Event, polling_time: int, heartbeat_interval: int
    ):
        multiprocessing.Process.__init__(self, name="Worker")

        self._connector: Optional[Connector] = None
        self._address = address
        self._stop_event = stop_event
        self._polling_time = polling_time
        self._heartbeat_interval = heartbeat_interval

        self._heartbeat = None
        self._thread_stop_event = None

        self._cached_functions: Dict[bytes, Any] = {}

    def run(self) -> None:
        setup_logger()
        self._initialize()
        self._run_forever()

    def _run_forever(self):
        while not self._stop_event.is_set():
            time.sleep(0.1)
            continue

        self._thread_stop_event.set()
        self._connector.join()
        self._heartbeat.join()
        logging.info(f"{self._get_prefix()} exited")

    def _initialize(self):
        self._thread_stop_event = threading.Event()

        self._connector = Connector(
            prefix="W",
            address=self._address,
            stop_event=self._thread_stop_event,
            callback=self._on_receive,
            polling_time=self._polling_time,
        )
        self._heartbeat = WorkerHeartbeat(
            address=self._address,
            worker_identity=self._connector.identity,
            interval=self._heartbeat_interval,
            stop_event=self._thread_stop_event,
        )
        logging.info(f"{self._get_prefix()} started")

    def _on_receive(self, message_type: MessageType, data: Message):
        match data:
            case Task():
                self._process_task(data)
            case TaskCancel():
                self._process_task_cancel(data)
            case _:
                logging.exception(f"{self._get_prefix()} unsupported {message_type=} {data=}")

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
            logging.exception(f"{self._get_prefix()} error when processing {task=}:")
            self._connector.send(MessageType.TaskResult, TaskResult(task.task_id, TaskStatus.Failed, str(e).encode()))

    def _process_task_cancel(self, task: Task):
        # TODO: implement this
        pass

    def _get_prefix(self):
        return f"Worker[{self._connector.identity.decode()}]:"