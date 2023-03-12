from collections import defaultdict
from typing import Dict, Set

from scaled.scheduler.mixins import ClientManager, Looper


class VanillaClientManager(ClientManager, Looper):
    def __init__(self):
        self._task_id_to_client: Dict[bytes, bytes] = dict()
        self._client_to_task_ids: Dict[bytes, Set[bytes]] = defaultdict(set)

    async def on_task_new(self, client: bytes, task_id: bytes):
        self._task_id_to_client[task_id] = client
        self._client_to_task_ids[client].add(task_id)

    async def on_task_cancel(self, client: bytes, task_id: bytes):
        pass

    async def on_task_done(self, task_id: bytes):
        client_id = self._task_id_to_client.pop(task_id)
        self._client_to_task_ids[client_id].remove(task_id)

    async def routine(self):
        pass

    async def statistics(self) -> Dict:
        return {client.hex(): len(task_ids) for client, task_ids in self._client_to_task_ids.items()}
