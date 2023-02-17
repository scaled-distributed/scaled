import asyncio
import queue
import threading

import zmq

from scaled.utility.event_loop import register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.worker.agent.agent_async import AgentAsync


class AgentSync(threading.Thread):
    def __init__(
        self,
        stop_event: threading.Event,
        context: zmq.Context,
        address: ZMQConfig,
        receive_task_queue: queue.Queue,
        send_task_queue: queue.Queue,
        heartbeat_interval_seconds: int,
        function_retention_seconds: int,
        event_loop: str,
    ):
        threading.Thread.__init__(self)
        self._event_loop = event_loop

        self._agent = AgentAsync(
            stop_event=stop_event,
            context=context,
            address=address,
            receive_task_queue=receive_task_queue,
            send_task_queue=send_task_queue,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            function_retention_seconds=function_retention_seconds,
        )

    def run(self) -> None:
        register_event_loop(self._event_loop)
        asyncio.run(self._agent.loop())
