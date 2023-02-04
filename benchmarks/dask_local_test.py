import random

from dask.distributed import Client as DaskClient, LocalCluster as DaskLocalCluster

from scaled.utility.logging.scoped_logger import ScopedLogger


def sleep_print(sec: int):
    return sec * 1


def main():
    tasks = [random.randint(0, 100) for i in range(100000)]

    cluster = DaskLocalCluster(n_workers=1, threads_per_worker=2, memory_limit="100GB")
    client = DaskClient(cluster)
    executor = client.get_executor()

    with ScopedLogger(f"submit {len(tasks)} tasks"):
        futures = [executor.submit(sleep_print, i) for i in tasks]

    with ScopedLogger(f"gather {len(futures)} results"):
        results = [future.result() for future in futures]

    assert results == tasks


if __name__ == "__main__":
    main()
