import argparse
import asyncio
import functools
import signal
import threading

from scaled.io.config import (
    DEFAULT_IO_THREADS,
    DEFAULT_FUNCTION_RETENTION_SECONDS,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaled.scheduler.main import Scheduler
from scaled.utility.event_loop import EventLoopType, register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.logging.utility import setup_logger

stop_event = threading.Event()


def get_args():
    parser = argparse.ArgumentParser("scaled scheduler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--io-threads", type=int, default=DEFAULT_IO_THREADS, help="number of io threads for zmq")
    parser.add_argument(
        "--worker-timeout-seconds",
        "-wt",
        type=int,
        default=DEFAULT_WORKER_TIMEOUT_SECONDS,
        help="discard worker when timeout seconds reached",
    )
    parser.add_argument(
        "--function-retention-seconds",
        "-ft",
        type=int,
        default=DEFAULT_FUNCTION_RETENTION_SECONDS,
        help="discard function in scheduler when timeout seconds " "reached",
    )
    parser.add_argument(
        "--per-worker-queue-size",
        "-qs",
        type=int,
        default=DEFAULT_PER_WORKER_QUEUE_SIZE,
        help="specify per worker queue size",
    )
    parser.add_argument(
        "--event-loop", "-e", default="builtin", choices=EventLoopType.allowed_types(), help="select event loop type"
    )
    parser.add_argument("address", type=ZMQConfig.from_string, help="scheduler address to connect to")

    return parser.parse_args()


def main():
    args = get_args()
    setup_logger()

    scheduler = Scheduler(
        address=args.address,
        io_threads=args.io_threads,
        per_worker_queue_size=args.per_worker_queue_size,
        worker_timeout_seconds=args.worker_timeout_seconds,
        function_retention_seconds=args.function_retention_seconds,
    )
    register_event_loop(args.event_loop)

    loop = asyncio.get_event_loop()
    __register_signal(loop)
    for coroutine in scheduler.get_loops():
        loop.create_task(coroutine())
    loop.run_forever()


def __register_signal(loop):
    loop.add_signal_handler(signal.SIGINT, functools.partial(__handle_signal, loop))
    loop.add_signal_handler(signal.SIGHUP, functools.partial(__handle_signal, loop))
    loop.add_signal_handler(signal.SIGTERM, functools.partial(__handle_signal, loop))


def __handle_signal(loop):
    for task in asyncio.all_tasks():
        task.cancel()
    loop.stop()
