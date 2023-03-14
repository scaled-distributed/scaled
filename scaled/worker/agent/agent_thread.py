import asyncio
import threading

import zmq

from scaled.utility.event_loop import register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.agent.agent import Agent


class AgentThread(threading.Thread):
    def __init__(
        self,
        external_address: ZMQConfig,
        internal_context: zmq.Context,
        internal_address: ZMQConfig,
        heartbeat_interval_seconds: int,
        function_retention_seconds: int,
        processing_queue_size: int,
        event_loop: str,
    ):
        threading.Thread.__init__(self)
        self._external_address=external_address
        self._internal_context=internal_context
        self._internal_address=internal_address
        self._heartbeat_interval_seconds=heartbeat_interval_seconds
        self._function_retention_seconds=function_retention_seconds
        self._processing_queue_size=processing_queue_size
        self._event_loop = event_loop

        self._loop = None
        self._task = None
        self._agent = None


    def run(self) -> None:
        register_event_loop(self._event_loop)
        self._loop = asyncio.new_event_loop()
        self._task = self._loop.create_task(self._main())
        self._loop.run_until_complete(self._task)

    def terminate(self):
        self._task.cancel()

    async def _main(self):
        self._agent = Agent(
            external_address=self._external_address,
            internal_context=self._internal_context,
            internal_address=self._internal_address,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            function_retention_seconds=self._function_retention_seconds,
            processing_queue_size=self._processing_queue_size,
        )

        try:
            await self._agent.get_loops()
        except asyncio.CancelledError:
            pass