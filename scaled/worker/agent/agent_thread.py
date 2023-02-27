import asyncio
import threading

import zmq

from scaled.utility.event_loop import register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.agent.agent_async import AgentAsync


class AgentThread(threading.Thread):
    def __init__(
        self,
        external_address: ZMQConfig,
        internal_context: zmq.Context,
        internal_address: ZMQConfig,
        heartbeat_interval_seconds: int,
        function_retention_seconds: int,
        event_loop: str,
    ):
        threading.Thread.__init__(self)
        self._event_loop = event_loop
        self._loop = None

        self._agent = AgentAsync(
            external_address=external_address,
            internal_context=internal_context,
            internal_address=internal_address,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            function_retention_seconds=function_retention_seconds,
        )

    def run(self) -> None:
        register_event_loop(self._event_loop)

        self._loop = asyncio.new_event_loop()
        for coroutine in self._agent.get_loops():
            self._loop.create_task(coroutine())

        self._loop.run_forever()

    def terminate(self):
        self._loop.stop()
