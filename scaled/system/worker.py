import logging
import multiprocessing
import pickle
import threading
from collections import deque
from typing import List, Optional

from scaled.system.io.heartbeat import start_heartbeat
from scaled.system.config import ZMQConfig
from scaled.system.io.connector import Connector
from scaled.system.objects import MessageType
from scaled.system.protocol import BACKEND_PROTOCOL, TaskClass


class Worker(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        address: ZMQConfig,
        stop_event: multiprocessing.Event,
        polling_time: int = 1000,
        heartbeat_interval: int = 1,
    ):
        multiprocessing.Process.__init__(self, name="worker")

        self._connector: Optional[Connector] = None
        self._address = address
        self._stop_event = stop_event
        self._polling_time = polling_time
        self._heartbeat_interval = heartbeat_interval

        self._heartbeat = None
        self._heartbeat_stop_event = None

        self._functions = {}
        self._task_queue = deque()

    def run(self) -> None:
        self._initialize()
        self._run_forever()

    def _run_forever(self):
        while not self._stop_event.is_set():
            self._on_receive(self._connector.receive())

        self._heartbeat_stop_event.set()

    def _initialize(self):
        self._connector = Connector("Worker", self._address, self._stop_event, self._polling_time)
        self._heartbeat_stop_event = threading.Event()
        self._heartbeat = start_heartbeat(
            address=self._address,
            worker_identity=self._connector.identity,
            interval=self._heartbeat_interval,
            stop_event=self._heartbeat_stop_event,
        )

    def _on_receive(self, frames: List[bytes]):
        source, message_type, *payload = frames
        obj = BACKEND_PROTOCOL[message_type](*payload)

        if message_type == MessageType.FunctionAddInstruction.value:
            self._functions[obj.function_name] = pickle.loads(obj.function)
        elif message_type == MessageType.FunctionDeleteInstruction.value:
            self._functions.pop(pickle.loads(obj.function_name))
        elif message_type == MessageType.Task:
            self._process_task(obj)

    def _request_function(self, function_name: bytes):
        self._connector.send([MessageType.FunctionRequest.value, function_name])

    def _process_task(self, task: TaskClass):
        self._task_queue.append(task)

        if task.function_name not in self._functions:
            self._request_function(task.function_name)
            return

        while self._task_queue:
            # noinspection PyBroadException
            try:
                task = self._task_queue.popleft()
                result = pickle.dumps(self._functions[task.function_name](*pickle.loads(task.args)))
                self._connector.send([MessageType.TaskResult.value, task.task_id, result])
            except Exception:
                logging.exception(f"error when processing {task=}:")
