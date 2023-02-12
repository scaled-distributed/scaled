import argparse
import multiprocessing
import os

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
        "--num-of-workers", "-n", type=int, default=os.cpu_count() - 1, help="number of workers in cluster"
    )
    parser.add_argument(
        "--heartbeat-interval", type=int, default=10, help="number of seconds to send heartbeat interval"
    )
    parser.add_argument(
        "--function-retention-seconds", type=int, default=10, help="number of seconds function cached in worker process"
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
        event_loop=args.event_loop,
        serializer=DefaultSerializer(),
    )
    cluster.run()
