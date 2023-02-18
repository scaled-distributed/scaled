import enum
import logging


class EventLoopType(enum.Enum):
    builtin = enum.auto()
    uvloop = enum.auto()

    @staticmethod
    def allowed_types():
        return {m.name for m in EventLoopType}


def register_event_loop(event_loop_type: str):
    if event_loop_type not in EventLoopType.allowed_types():
        raise TypeError(f"allowed event loop types are: {EventLoopType.allowed_types()}")

    event_loop_type = EventLoopType[event_loop_type]
    if event_loop_type == EventLoopType.uvloop:
        try:
            import uvloop  # noqa
        except ImportError:
            raise ImportError(f"please use pip install uvloop if try to use uvloop as event loop")

        uvloop.install()

    logging.info(f"use event loop: {event_loop_type.value}")
