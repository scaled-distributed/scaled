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

    cluster = SchedulerClusterCombo(address=address, n_workers=10, per_worker_queue_size=2, event_loop="uvloop")
    client = Client(address=address)

    tasks = [random.randint(0, 101) for _ in range(10000)]

    with ScopedLogger(f"scaled submit {len(tasks)} tasks"):
        futures = [client.submit(sleep_print, a) for a in tasks]

    with ScopedLogger(f"scaled gather {len(futures)} results"):
        results = [future.result() for future in futures]

    assert results == tasks

    client.disconnect()
    cluster.shutdown()


if __name__ == "__main__":
    main()
