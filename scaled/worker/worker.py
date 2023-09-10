import asyncio
import logging
import multiprocessing
import signal
from typing import Optional

import zmq.asyncio

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import BalanceRequest
from scaled.protocol.python.message import BalanceResponse
from scaled.protocol.python.message import DisconnectRequest
from scaled.protocol.python.message import FunctionRequest
from scaled.protocol.python.message import FunctionResponse
from scaled.protocol.python.message import HeartbeatEcho
from scaled.protocol.python.message import MessageVariant
from scaled.protocol.python.message import Task
from scaled.protocol.python.message import TaskCancel
from scaled.protocol.python.serializer.mixins import Serializer
from scaled.utility.event_loop import create_async_loop_routine
from scaled.utility.event_loop import register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.agent.heartbeat_manager import VanillaHeartbeatManager
from scaled.worker.agent.processor_manager import VanillaProcessorManager
from scaled.worker.agent.task_manager import VanillaTaskManager
from scaled.worker.agent.timeout_manager import VanillaTimeoutManager

# from scaled.utility.logging.utility import setup_logger


class Worker(multiprocessing.get_context("spawn").Process):  # type: ignore
    def __init__(
        self,
        event_loop: str,
        address: ZMQConfig,
        heartbeat_interval_seconds: int,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        serializer: Serializer,
        function_retention_seconds: int,
        death_timeout_seconds: int,
    ):
        multiprocessing.Process.__init__(self, name="Agent")

        self._event_loop = event_loop
        self._address = address

        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._serializer = serializer
        self._function_retention_seconds = function_retention_seconds
        self._death_timeout_seconds = death_timeout_seconds

        self._connector_external: Optional[AsyncConnector] = None
        self._task_manager: Optional[VanillaTaskManager] = None
        self._heartbeat: Optional[VanillaHeartbeatManager] = None
        self._processor_manager: Optional[VanillaProcessorManager] = None

    @property
    def identity(self):
        return self._connector_external.identity

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def __initialize(self):
        # setup_logger()
        register_event_loop(self._event_loop)

        self._connector_external = AsyncConnector(
            context=zmq.asyncio.Context(),
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
        )

        self._heartbeat = VanillaHeartbeatManager()
        self._timeout_manager = VanillaTimeoutManager(death_timeout_seconds=self._death_timeout_seconds)
        self._task_manager = VanillaTaskManager()
        self._processor_manager = VanillaProcessorManager(
            event_loop=self._event_loop,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            function_retention_seconds=self._function_retention_seconds,
            serializer=self._serializer,
        )

        # register
        self._task_manager.register(connector=self._connector_external, processor_manager=self._processor_manager)
        self._heartbeat.register(
            connector_external=self._connector_external,
            worker_task_manager=self._task_manager,
            timeout_manager=self._timeout_manager,
            processor_manager=self._processor_manager,
        )
        self._processor_manager.register(
            heartbeat=self._heartbeat, task_manager=self._task_manager, connector_external=self._connector_external
        )
        self._processor_manager.initialize()

        self._loop = asyncio.get_event_loop()
        self.__register_signal()
        self._task = self._loop.create_task(self.__get_loops())

    async def __on_receive_external(self, message: MessageVariant):
        if isinstance(message, HeartbeatEcho):
            await self._heartbeat.on_heartbeat_echo(message)
            return

        if isinstance(message, Task):
            await self._task_manager.on_task_new(message)
            return

        if isinstance(message, TaskCancel):
            await self._task_manager.on_cancel_task(message)
            return

        if isinstance(message, BalanceRequest):
            task_ids = self._task_manager.on_balance_request(message)
            await self._connector_external.send(BalanceResponse(task_ids))
            return

        if isinstance(message, FunctionRequest):
            await self._processor_manager.on_function_request(message)
            return

        if isinstance(message, FunctionResponse):
            await self._processor_manager.on_function_response(message)
            return

        raise TypeError(f"Unknown {message=}")

    async def __get_loops(self):
        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._heartbeat.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._timeout_manager.routine, 1),
                create_async_loop_routine(self._task_manager.routine, 0),
                create_async_loop_routine(self._processor_manager.routine, 0),
            )
        except asyncio.CancelledError:
            pass
        except TimeoutError as e:
            logging.info(f"Worker[{self.pid}]: {str(e)}")

        await self._connector_external.send(DisconnectRequest(self._connector_external.identity))

        self._connector_external.destroy()
        self._processor_manager.destroy()
        logging.info(f"Worker[{self.pid}]: quited")

    def __run_forever(self):
        self._loop.run_until_complete(self._task)

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)

    def __destroy(self):
        self._task.cancel()
