import logging
import multiprocessing
import pickle
import threading
from collections import deque
from typing import List, Optional

from scaled.io.heartbeat import start_heartbeat
from scaled.io.config import ZMQConfig
from scaled.io.connector import Connector
from scaled.io.objects import MessageType, TaskStatus
from scaled.protocol.python import PROTOCOL, WorkerTask, FunctionRequest, WorkerTaskResult


class SingleWorker(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        address: ZMQConfig,
        stop_event: multiprocessing.Event,
        polling_time: int,
        heartbeat_interval: int,
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

    def _initialize(self):
        self._connector = Connector("W", self._address, self._stop_event, self._polling_time)
        self._heartbeat_stop_event = threading.Event()
        self._heartbeat = start_heartbeat(
            address=self._address,
            worker_identity=self._connector.identity,
            interval=self._heartbeat_interval,
            stop_event=self._heartbeat_stop_event,
        )

    def _on_receive(self, frames: List[bytes]):
        try:
            source, message_type, *payload = frames
            obj = PROTOCOL[message_type].deserialize(*payload)

            match message_type:
                case MessageType.WorkerFunctionAdd.value:
                    self._functions[obj.function_name] = pickle.loads(obj.function)
                case MessageType.WorkerFunctionDelete.value:
                    self._functions.pop(pickle.loads(obj.function_name))
                case MessageType.WorkerTask:
                    self._process_task(obj)
        except Exception as e:
            logging.exception(f"{self._connector.identity}: critical exception\n{e}")
        finally:
            self._heartbeat_stop_event.join()

    def _request_function(self, function_name: bytes):
        self._connector.send(MessageType.WorkerFunctionRequest, FunctionRequest(function_name))

    def _process_task(self, task: WorkerTask):
        self._task_queue.append(task)

        if task.function_name not in self._functions:
            self._request_function(task.function_name)
            return

        while self._task_queue:
            # noinspection PyBroadException
            try:
                task = self._task_queue.popleft()
                result = pickle.dumps(self._functions[task.function_name](*pickle.loads(task.args)))
                self._connector.send(
                    MessageType.WorkerTaskResult, WorkerTaskResult(task.task_id, TaskStatus.Success, result)
                )
            except Exception as e:
                logging.exception(f"error when processing {task=}:")
                self._connector.send(
                    MessageType.WorkerTaskResult, WorkerTaskResult(task.task_id, TaskStatus.Failed, str(e).encode())
                )
