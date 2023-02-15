import argparse
import multiprocessing

from scaled.io.config import (
    DEFAULT_FUNCTION_RETENTION_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_NUMBER_OF_WORKER,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
)
from scaled.protocol.python.serializer.default import DefaultSerializer
from scaled.utility.event_loop import EventLoopType, register_event_loop
from scaled.utility.zmq_config import ZMQConfig
from scaled.cluster.cluster import ClusterProcess

stop_event = multiprocessing.get_context("spawn").Event()


def get_args():
    parser = argparse.ArgumentParser(
        "standalone compute cluster", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--num-of-workers", "-n", type=int, default=DEFAULT_NUMBER_OF_WORKER, help="number of workers in cluster"
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=int,
        default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        help="number of seconds to send heartbeat interval",
    )
    parser.add_argument(
        "--function-retention-seconds",
        type=int,
        default=DEFAULT_FUNCTION_RETENTION_SECONDS,
        help="number of seconds function cached in worker process",
    )
    parser.add_argument(
        "--garbage-collect-interval-seconds",
        type=int,
        default=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        help="garbage collect interval seconds",
    )
    parser.add_argument(
        "--trim-memory-threshold-bytes",
        type=int,
        default=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        help="number of bytes threshold to enable libc to trim memory",
    )
    parser.add_argument(
        "--event-loop", default="builtin", choices=EventLoopType.allowed_types(), help="select event loop type"
    )
    parser.add_argument("address", type=ZMQConfig.from_string, help="scheduler address to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    register_event_loop(args.event_loop)

    cluster = ClusterProcess(
        stop_event=stop_event,
        address=args.address,
        n_workers=args.num_of_workers,
        heartbeat_interval_seconds=args.heartbeat_interval,
        function_retention_seconds=args.function_retention_seconds,
        garbage_collect_interval_seconds=args.garbage_collect_interval_seconds,
        trim_memory_threshold_bytes=args.trim_memory_threshold_bytes,
        event_loop=args.event_loop,
        serializer=DefaultSerializer(),
    )
    cluster.run()
