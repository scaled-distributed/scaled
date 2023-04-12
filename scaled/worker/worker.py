import asyncio
import multiprocessing
import signal
from typing import Optional

import zmq.asyncio

from scaled.io.async_connector import AsyncConnector
from scaled.io.config import CLEANUP_INTERVAL_SECONDS
from scaled.protocol.python.message import DisconnectRequest, MessageType, MessageVariant
from scaled.protocol.python.serializer.mixins import FunctionSerializerType
from scaled.utility.event_loop import create_async_loop_routine, register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.agent.function_cache import VanillaFunctionCacheManager
from scaled.worker.agent.heartbeat import VanillaHeartbeatManager
from scaled.worker.agent.task_manager import VanillaTaskManager
from scaled.worker.agent.processor_manager import VanillaProcessorManager


class Worker(multiprocessing.get_context("spawn").Process):
    def __init__(
        self,
        event_loop: str,
        address: ZMQConfig,
        heartbeat_interval_seconds: int,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        serializer: FunctionSerializerType,
        function_retention_seconds: int,
    ):
        multiprocessing.Process.__init__(self, name="Agent")

        self._event_loop = event_loop
        self._address = address

        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._serializer = serializer
        self._function_retention_seconds = function_retention_seconds

        self._connector_external: Optional[AsyncConnector] = None
        self._task_manager: Optional[VanillaTaskManager] = None
        self._function_cache: Optional[VanillaFunctionCacheManager] = None
        self._heartbeat: Optional[VanillaHeartbeatManager] = None
        self._processor_manager: Optional[VanillaProcessorManager] = None

    @property
    def identity(self):
        return self._connector_external.identity

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def __initialize(self):
        register_event_loop(self._event_loop)

        self._connector_external = AsyncConnector(
            context=zmq.asyncio.Context(),
            prefix="W",
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
        )

        self._function_cache = VanillaFunctionCacheManager(function_retention_seconds=self._function_retention_seconds)
        self._task_manager = VanillaTaskManager()
        self._heartbeat = VanillaHeartbeatManager()
        self._processor_manager = VanillaProcessorManager(
            event_loop=self._event_loop,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            serializer=self._serializer,
        )

        # register
        self._function_cache.register(
            connector_external=self._connector_external,
            task_manager=self._task_manager,
            internal_manager=self._processor_manager,
        )
        self._task_manager.register(processor_manager=self._processor_manager)
        self._heartbeat.register(connector_external=self._connector_external, worker_task_manager=self._task_manager)
        self._processor_manager.register(heartbeat=self._heartbeat, function_cache=self._function_cache)

        self._processor_manager.restart_processor()
        self._loop = asyncio.new_event_loop()
        self.__register_signal()
        self._task = self._loop.create_task(self.__get_loops())

    async def __on_receive_external(self, message_type: MessageType, message: MessageVariant):
        if message_type == MessageType.Task:
            await self._function_cache.on_new_task(message)
            return

        if message_type == MessageType.TaskCancel:
            await self._function_cache.on_cancel_task(message)
            return

        if message_type == MessageType.BalanceRequest:
            await self._function_cache.on_balance_request(message)
            return

        if message_type == MessageType.FunctionResponse:
            await self._function_cache.on_new_function(message)
            return

        raise TypeError(f"Unknown {message_type=} {message=}")

    async def __get_loops(self):
        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._function_cache.routine, CLEANUP_INTERVAL_SECONDS),
                create_async_loop_routine(self._task_manager.routine, 0),
                create_async_loop_routine(self._heartbeat.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._processor_manager.routine, 0),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass

        await self._connector_external.send(
            MessageType.DisconnectRequest, DisconnectRequest(self._connector_external.identity)
        )

    def __run_forever(self):
        self._loop.run_until_complete(self._task)

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__shutdown)
        self._loop.add_signal_handler(signal.SIGTERM, self.__shutdown)

    def __shutdown(self):
        self._processor_manager.shutdown()
        self._task.cancel()
