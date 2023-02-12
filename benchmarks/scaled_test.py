import random

from scaled.client import Client
from scaled.cluster.combo import SchedulerClusterCombo

from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def sleep_print(sec: int):
    return sec * 1


def main():
    setup_logger()

    address = "tcp://127.0.0.1:2345"

    cluster = SchedulerClusterCombo(address=address, n_workers=10, event_loop="uvloop")
    client = Client(address=address)

    tasks = [random.randint(0, 100) for _ in range(100000)]

    with ScopedLogger(f"submit {len(tasks)} tasks"):
        futures = [client.submit(sleep_print, i) for i in tasks]

    with ScopedLogger(f"gather {len(futures)} results"):
        results = [future.result() for future in futures]

    assert results == tasks

    client.disconnect()
    cluster.shutdown()


if __name__ == "__main__":
    main()
