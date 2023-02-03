from typing import Dict

from scaled.protocol.python.message import FunctionAdd
from scaled.scheduler.mixins import FunctionManager


class VanillaFunctionManager(FunctionManager):
    def __init__(self):
        pass

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
