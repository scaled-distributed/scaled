import random

from dask.distributed import Client as DaskClient, LocalCluster as DaskLocalCluster

from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger


def sleep_print(sec: int):
    return sec * 1


def main():
    setup_logger()
    tasks = [random.randint(0, 100) for _ in range(10000)]

    cluster = DaskLocalCluster(n_workers=10, threads_per_worker=2, memory_limit="100GB")
    client = DaskClient(cluster)
    executor = client.get_executor()

    with ScopedLogger(f"submit {len(tasks)} tasks"):
        futures = [executor.submit(sleep_print, i) for i in tasks]

    with ScopedLogger(f"gather {len(futures)} results"):
        results = [future.result() for future in futures]

    assert results == tasks


if __name__ == "__main__":
    main()
