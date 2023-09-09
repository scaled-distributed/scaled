import asyncio
import enum
import logging
from typing import Any
from typing import Callable
from typing import Coroutine


class EventLoopType(enum.Enum):
    builtin = "builtin"
    uvloop = "uvloop"

    @staticmethod
    def allowed_types():
        return {m.name for m in EventLoopType}


def register_event_loop(event_loop_type_str: str):
    if event_loop_type_str not in EventLoopType.allowed_types():
        raise TypeError(f"allowed event loop types are: {EventLoopType.allowed_types()}")

    event_loop_type: EventLoopType = EventLoopType(event_loop_type_str)
    if event_loop_type == EventLoopType.uvloop:
        try:
            import uvloop  # noqa
        except ImportError:
            raise ImportError("please use pip install uvloop if try to use uvloop as event loop")

        uvloop.install()

    logging.info(f"use event loop: {event_loop_type.value}")


def create_async_loop_routine(routine: Callable[[], Coroutine[Any, Any, Any]], seconds: int):
    async def loop():
        logging.info(f"{routine.__self__.__class__.__name__}: started")  # type: ignore
        try:
            while True:
                await routine()
                await asyncio.sleep(seconds)
        except asyncio.CancelledError as e:
            logging.info(f"{routine.__self__.__class__.__name__}: exited")  # type: ignore
            raise e

    return loop()
