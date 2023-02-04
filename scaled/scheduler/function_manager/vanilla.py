from typing import Dict, Set

from scaled.protocol.python.message import FunctionAdd
from scaled.scheduler.mixins import FunctionManager


class VanillaFunctionManager(FunctionManager):
    def __init__(self):
        self._function_id_to_task_ids: Dict[bytes, Set[bytes]] = dict()

    async def on_function_add(self, function: FunctionAdd):
        pass

    async def on_function_request(self, function_name: bytes):
        pass

    async def on_function_check(self, function_name: bytes) -> bool:
        pass

    async def routine(self):
        pass

    async def statistics(self) -> Dict:
        pass
