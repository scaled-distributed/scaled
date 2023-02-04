import argparse
import multiprocessing
import os
import signal

from scaled.cluster.standalone_cluster import StandaloneCluster
from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.logging.utility import setup_logger

stop_event = multiprocessing.get_context("spawn").Event()


def get_args():
    parser = argparse.ArgumentParser(
        "standalone compute cluster", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--num-of-workers", "-n", type=int, default=os.cpu_count() - 1, help="number of workers in " "cluster"
    )
    parser.add_argument(
        "--heartbeat-interval", type=int, default=10, help="number of seconds to send heartbeat " "interval"
    )
    parser.add_argument("address", type=ZMQConfig.from_string, help="scheduler address to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    setup_logger()

    __register_signal()
    cluster = StandaloneCluster(
        address=args.address,
        n_workers=args.num_of_workers,
        heartbeat_interval=args.heartbeat_interval,
        stop_event=stop_event,
    )
    cluster.join()


def __register_signal():
    signal.signal(signal.SIGINT, __handle_signal)
    signal.signal(signal.SIGTERM, __handle_signal)


def __handle_signal(*args):
    stop_event.set()
