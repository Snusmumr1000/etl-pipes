from dataclasses import dataclass, field
from typing import Protocol, TypeVar

Key = TypeVar("Key")
Value = TypeVar("Value")


@dataclass
class Cache(Protocol[Key, Value]):
    state: dict[Key, Value] = field(default_factory=dict)

    def set(self, key: Key, value: Value) -> None:
        self.state[key] = value

    def get(self, key: Key) -> Value | None:
        return self.state.get(key)

    def delete(self, key: Key) -> None:
        if key in self.state:
            del self.state[key]
