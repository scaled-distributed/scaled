import argparse
import asyncio
import functools
import signal

from scaled.io.config import (
    DEFAULT_IO_THREADS,
    DEFAULT_FUNCTION_RETENTION_SECONDS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaled.scheduler.main import scheduler_main
from scaled.utility.event_loop import EventLoopType, register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.logging.utility import setup_logger


def get_args():
    parser = argparse.ArgumentParser("scaled scheduler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--io-threads", type=int, default=DEFAULT_IO_THREADS, help="number of io threads for zmq")
    parser.add_argument(
        "--max-number-of-tasks-waiting",
        "-mt",
        type=int,
        default=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
        help="max number of tasks can wait in scheduler " "while all workers are full",
    )
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
        help="discard function in scheduler when timeout seconds reached",
    )
    parser.add_argument(
        "--load-balance-seconds",
        "-ls",
        type=int,
        default=DEFAULT_LOAD_BALANCE_SECONDS,
        help="number of seconds for load balance operation in scheduler",
    )
    parser.add_argument(
        "--load-balance-trigger-times",
        "-lbt",
        type=int,
        default=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
        help="exact number of repeated load balance advices when trigger load balance operation in scheduler",
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

    register_event_loop(args.event_loop)

    loop = asyncio.get_event_loop()
    __register_signal(loop)
    loop.run_until_complete(
        scheduler_main(
            address=args.address,
            io_threads=args.io_threads,
            max_number_of_tasks_waiting=args.max_number_of_tasks_waiting,
            per_worker_queue_size=args.per_worker_queue_size,
            worker_timeout_seconds=args.worker_timeout_seconds,
            function_retention_seconds=args.function_retention_seconds,
            load_balance_seconds=args.load_balance_seconds,
            load_balance_trigger_times=args.load_balance_trigger_times,
        )
    )


def __register_signal(loop):
    loop.add_signal_handler(signal.SIGINT, functools.partial(__handle_signal))
    loop.add_signal_handler(signal.SIGTERM, functools.partial(__handle_signal))


def __handle_signal():
    for task in asyncio.all_tasks():
        task.cancel()
