from typing import List

from scaled.system.io.binder import Binder
from scaled.system.objects import HeartbeatInfo, Message Type
from scaled.system.protocol import BACKEND_PROTOCOL, FRONTEND_PROTOCOL
from scaled.system.scheduler.mixins import Scheduler


class Engine:
    def __init__(self, binder: Binder, scheduler: Scheduler):
        self._binder = binder
        self._binder.register(self.on_receive_message)

        self._scheduler = scheduler

    async def on_receive_message(self, source: bytes, message_type: bytes, frames: List[bytes]):
        if message_type in FRONTEND_PROTOCOL:
            pass
        elif message_type in BACKEND_PROTOCOL:
            pass

