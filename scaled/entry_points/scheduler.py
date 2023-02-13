import argparse
import asyncio
import signal
import threading

from scaled.scheduler.main import Scheduler
from scaled.utility.event_loop import EventLoopType, register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.logging.utility import setup_logger

stop_event = threading.Event()


def get_args():
    parser = argparse.ArgumentParser("scaled scheduler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--worker-timeout-seconds", "-wt", type=int, default=60, help="discard worker when timeout seconds reached"
    )
    parser.add_argument(
        "--function-retention-seconds",
        "-ft",
        type=int,
        default=60,
        help="discard function in scheduler when timeout seconds " "reached",
    )
    parser.add_argument("--per-worker-queue-size", "-qs", type=int, default=2, help="specify per worker queue size")
    parser.add_argument(
        "--event-loop", "-e", default="builtin", choices=EventLoopType.allowed_types(), help="select event loop type"
    )
    parser.add_argument("address", type=ZMQConfig.from_string, help="scheduler address to connect to")

    return parser.parse_args()


def main():
    args = get_args()
    setup_logger()

    __register_signal()

    scheduler = Scheduler(
        address=args.address,
        stop_event=stop_event,
        per_worker_queue_size=args.per_worker_queue_size,
        worker_timeout_seconds=args.worker_timeout_seconds,
        function_retention_seconds=args.function_retention_seconds,
    )
    register_event_loop(args.event_loop)
    asyncio.run(scheduler.loop())


def __register_signal():
    signal.signal(signal.SIGINT, __handle_signal)
    signal.signal(signal.SIGTERM, __handle_signal)


def __handle_signal(*args):
    assert args is not None
    stop_event.set()
