from collections.abc import Sequence
from dataclasses import dataclass
from typing import NewType

ItemId = NewType("ItemId", str)
TodoId = NewType("TodoId", str)


@dataclass
class Item:
    id: ItemId
    name: str
    description: str
    done: bool
    order: int


@dataclass
class Todo:
    id: TodoId
    name: str
    description: str
    items: Sequence[Item]
