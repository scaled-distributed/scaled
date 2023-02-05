import argparse
import asyncio
import signal
import threading

import uvloop

from scaled.scheduler.router import Router
from scaled.scheduler.worker_manager.vanilla import AllocatorType
from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.logging.utility import setup_logger

stop_event = threading.Event()


def get_args():
    parser = argparse.ArgumentParser("scaled scheduler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--worker-timeout-seconds", type=int, default=60, help="discard worker when timeout seconds " "reached"
    )
    parser.add_argument(
        "--function-timeout-seconds",
        type=int,
        default=60,
        help="discard function in scheduler when timeout seconds " "reached",
    )
    parser.add_argument(
        "--allocator-type",
        required=True,
        type=AllocatorType,
        choices={t for t in AllocatorType},
        help="specify allocator type",
    )
    parser.add_argument("address", type=ZMQConfig.from_string, help="scheduler address to connect to")

    return parser.parse_args()


def main():
    args = get_args()
    setup_logger()

    __register_signal()

    router = Router(
        address=args.address,
        stop_event=stop_event,
        allocator_type=args.allocator_type,
        worker_timeout_seconds=args.worker_timeout_seconds,
        function_timeout_seconds=args.function_timeout_seconds,
    )
    uvloop.install()
    asyncio.run(router.loop())


def __register_signal():
    signal.signal(signal.SIGINT, __handle_signal)
    signal.signal(signal.SIGTERM, __handle_signal)


def __handle_signal(*args):
    stop_event.set()
