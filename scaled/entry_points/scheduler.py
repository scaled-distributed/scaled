import argparse
import asyncio
import signal
import threading

import uvloop

from scaled.scheduler.router import Router
from scaled.utility.zmq_config import ZMQConfig
from scaled.utility.logging.utility import setup_logger

stop_event = threading.Event()


def get_args():
    parser = argparse.ArgumentParser("scaled scheduler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("address", type=ZMQConfig.from_string, help="scheduler address to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    setup_logger()

    __register_signal()

    router = Router(address=args.address, stop_event=stop_event)
    uvloop.install()
    asyncio.run(router.loop())


def __register_signal():
    signal.signal(signal.SIGINT, __handle_signal)
    signal.signal(signal.SIGTERM, __handle_signal)


def __handle_signal(*args):
    stop_event.set()
