from typing import List

from scaled.system.io.binder import Binder
from scaled.system.objects import HeartbeatInfo, MessageType
from scaled.system.scheduler.mixins import Scheduler


class Engine:
    def __init__(self, frontend: Binder, scheduler: Scheduler, backend: Binder):
        self._frontend = frontend
        self._frontend.register(self.on_client_message)

        self._scheduler = scheduler

        self._backend = backend
        self._backend.register(self.on_worker_message)

    async def on_client_message(self, frames: List[bytes]):
        pass

    async def on_worker_message(self, frames: List[bytes]):
        source, message_type, *data = frames
        if message_type == MessageType.Info.value:
            worker, heartbeat = data
            await self._scheduler.on_heartbeat(worker, HeartbeatInfo.from_bytes(heartbeat))

