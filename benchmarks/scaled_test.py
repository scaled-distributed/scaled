import random

from scaled.client.client import Client
from scaled.cluster.combo import SchedulerClusterCombo
from scaled.utility.zmq_config import ZMQConfig, ZMQType
from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def sleep_print(sec: int):
    return sec * 1


def main():
    setup_logger()
    config = ZMQConfig(type=ZMQType.tcp, host="127.0.0.1", port=2345)

    cluster = SchedulerClusterCombo(address=config, n_workers=10, event_loop="uvloop")
    client = Client(config=config)

    tasks = [random.randint(0, 100) for _ in range(100000)]

    with ScopedLogger(f"submit {len(tasks)} tasks"):
        futures = [client.submit(sleep_print, i) for i in tasks]

    with ScopedLogger(f"gather {len(futures)} results"):
        results = [future.result() for future in futures]

    assert results == tasks

    cluster.shutdown()
    client.disconnect()


if __name__ == "__main__":
    main()
