import logging
import multiprocessing
import pickle
import threading
from collections import deque
from typing import Any, Dict, List, Optional

from scaled.io.heartbeat import start_heartbeat
from scaled.io.config import ZMQConfig
from scaled.io.connector import Connector
from scaled.protocol.python.objects import MessageType, JobStatus
from scaled.protocol.python.message import (
    JobResult, Job, PROTOCOL, FunctionRequest,
)


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

        self._cached_functions: Dict[int, Dict[bytes, Any]] = {}
        self._map_job_queue = deque()

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
                case MessageType.FunctionResult.value:
                    self._cached_functions[obj.job_id][obj.function_name] = pickle.loads(obj.function)
                case MessageType.DeleteFunction.value:
                    self._cached_functions[obj.job_id].pop(obj.function_name)
                    if len(self._cached_functions[obj.job_id]) == 0:
                        self._cached_functions.pop(obj.job_id)
                case MessageType.Job:
                    self._process_map_job(obj)
                case _:
                    raise TypeError(f"Worker received unsupported message type: {message_type}")
        except Exception as e:
            logging.exception(f"{self._connector.identity}: critical exception\n{e}")
        finally:
            self._heartbeat_stop_event.join()

    def _process_map_job(self, job: Job):
        self._map_job_queue.append(job)

        if job.function_name not in self._cached_functions:
            self._request_function(job.job_id, job.function_name)
            return

        while self._map_job_queue:
            # noinspection PyBroadException
            try:
                job = self._map_job_queue.popleft()
                result = tuple(
                    pickle.dumps(self._cached_functions[job.job_id][job.function_name](*pickle.loads(args)))
                    for args in job.list_of_args
                )
                self._connector.send(
                    MessageType.JobResult, JobResult(job.task_id, JobStatus.Success, result)
                )
            except Exception as e:
                logging.exception(f"error when processing {job=}:")
                self._connector.send(
                    MessageType.JobResult, JobResult(job.task_id, JobStatus.Failed, (str(e).encode(),))
                )

    def _request_function(self, job_id: int, function_name: bytes):
        self._connector.send(MessageType.RequestFunction, FunctionRequest(job_id, function_name))
