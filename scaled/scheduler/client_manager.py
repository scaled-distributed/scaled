from typing import Dict
from typing import Optional
from typing import Set

from scaled.scheduler.mixins import ClientManager
from scaled.scheduler.mixins import Looper
from scaled.scheduler.mixins import Reporter
from scaled.utility.one_to_many_dict import OneToManyDict


class VanillaClientManager(ClientManager, Looper, Reporter):
    def __init__(self):
        self._client_to_task_ids: OneToManyDict[bytes, bytes] = OneToManyDict()

    def get_client_task_ids(self, client: bytes) -> Set[bytes]:
        return self._client_to_task_ids.get_values(client)

    def get_client_id(self, task_id: bytes) -> Optional[bytes]:
        return self._client_to_task_ids.get_key(task_id)

    async def on_task_new(self, client: bytes, task_id: bytes):
        self._client_to_task_ids.add(client, task_id)

    async def on_task_done(self, task_id: bytes) -> bytes:
        return self._client_to_task_ids.remove_value(task_id)

    async def routine(self):
        pass

    async def statistics(self) -> Dict:
        return {
            "client_manager": {client.decode(): len(task_ids) for client, task_ids in self._client_to_task_ids.items()}
        }
