from collections import defaultdict
from typing import Dict, Optional, Set

from scaled.scheduler.mixins import ClientManager, Looper, Reporter


class VanillaClientManager(ClientManager, Looper, Reporter):
    def __init__(self):
        self._task_id_to_client: Dict[bytes, bytes] = dict()
        self._client_to_task_ids: Dict[bytes, Set[bytes]] = defaultdict(set)

    def get_client_task_ids(self, client: bytes) -> Set[bytes]:
        return self._client_to_task_ids[client]

    def get_client_id(self, task_id: bytes) -> Optional[bytes]:
        return self._task_id_to_client.get(task_id, None)

    async def on_task_new(self, client: bytes, task_id: bytes):
        self._task_id_to_client[task_id] = client
        self._client_to_task_ids[client].add(task_id)

    async def on_task_done(self, task_id: bytes) -> bytes:
        client_id = self._task_id_to_client.pop(task_id)
        self._client_to_task_ids[client_id].remove(task_id)
        return client_id

    async def routine(self):
        pass

    async def statistics(self) -> Dict:
        return {
            "client_manager": {client.hex(): len(task_ids) for client, task_ids in self._client_to_task_ids.items()}
        }
