import multiprocessing

from scaled.io.config import ZMQConfig, ZMQType
from scaled.worker.worker_master import WorkerMaster
from scaled.cluster.local.local_router import LocalRouter


class LocalCluster:
    def __init__(
        self,
        config: ZMQConfig,
        stop_event: multiprocessing.Event,
        n_workers: int,
        polling_time: int,
        heartbeat_interval: int,
    ):
        self._config = config

        self._inproc_config = ZMQConfig(type=ZMQType.inproc, host="demo")
        self._worker_master = WorkerMaster(
            address=config,
            stop_event=stop_event,
            n_workers=n_workers,
            polling_time=polling_time,
            heartbeat_interval=heartbeat_interval,
        )
        self._router = LocalRouter(config=config)

    def start(self):
        self._worker_master.start()
        self._router.start()
