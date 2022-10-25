import logging
import multiprocessing
import pickle
import threading
from typing import List, Optional

from scaled.system.io.heartbeat import start_heartbeat
from scaled.system.config import ZMQConfig
from scaled.system.io.connector import Connector
from scaled.system.objects import MessageType
from scaled.system.objects import UnitResult


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
        self._callback = None
        self._functions = {}

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
        source, message_type, *data = frames
        if message_type == MessageType.AddFunction.value:
            self._functions[pickle.loads(data[0])] = pickle.loads(data[1])
        elif message_type == MessageType.DelFunction.value:
            self._functions.pop(pickle.loads(data[0]))
        elif message_type == MessageType.Task:
            self._process_task(data)

    def _process_task(self, data: List[bytes]):
        function_name_raw, task_list_raw = data
        function_name = pickle.loads(function_name_raw)
        if function_name not in self._functions:
            logging.error(f"cannot find function `{function_name}` in worker")
            return

        task_list = pickle.loads(task_list_raw)
        result_payload = pickle.dumps(
            [UnitResult(id=task.id, result=self._functions[task.function_name](*task.args)) for task in task_list]
        )
        self._socket.send_multipart([MessageType.Result.value, result_payload])
