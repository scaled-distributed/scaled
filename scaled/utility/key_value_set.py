from typing import Dict
from typing import Generic
from typing import Set
from typing import TypeVar

KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")


class KeyValueDictSet(Generic[KeyT, ValueT]):
    def __init__(self):
        self._key_to_value_set: Dict[KeyT, Set[ValueT]] = dict()

    def __contains__(self, key) -> bool:
        return key in self._key_to_value_set

    def keys(self):
        return self._key_to_value_set.keys()

    def values(self):
        return self._key_to_value_set.values()

    def items(self):
        return self._key_to_value_set.items()

    def add(self, key: KeyT, value: ValueT):
        if key not in self._key_to_value_set:
            self._key_to_value_set[key] = set()

        self._key_to_value_set[key].add(value)

    def get_values(self, key: KeyT) -> Set[ValueT]:
        if key not in self._key_to_value_set:
            raise ValueError(f"cannot find {key=} in KeyValueSet")

        return self._key_to_value_set[key]

    def remove_key(self, key: KeyT) -> Set[ValueT]:
        if key not in self._key_to_value_set:
            raise KeyError(f"cannot find {key=} in KeyValueSet")

        values = self._key_to_value_set.pop(key)
        return values

    def remove_value(self, key: KeyT, value: ValueT):
        if key not in self._key_to_value_set:
            raise KeyError(f"cannot find {key=} in KeyValueSet")

        self._key_to_value_set[key].remove(value)
        if not self._key_to_value_set[key]:
            self._key_to_value_set.pop(key)
