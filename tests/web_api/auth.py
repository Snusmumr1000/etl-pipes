from dataclasses import dataclass
from enum import StrEnum
from typing import NewType

from fastapi import HTTPException

from etl_pipes.pipes.base_pipe import Pipe
from tests.web_api.domain_types import ItemId, TodoId

AuthToken = NewType("AuthToken", str)


class Ops(StrEnum):
    Create = "create"
    Read = "read"
    Update = "update"
    Delete = "delete"


@dataclass
class CheckAccessForItem(Pipe):
    token: AuthToken
    item_id: ItemId
    ops: list[Ops]

    async def __call__(self) -> ItemId | None:
        for op in self.ops:
            if op in self.token:
                return self.item_id
        raise HTTPException(status_code=403, detail="Not enough permissions")


@dataclass
class CheckAccessForTodo(Pipe):
    token: AuthToken
    todo_id: TodoId
    ops: list[Ops]

    async def __call__(self) -> TodoId | None:
        for op in self.ops:
            if op in self.token:
                return self.todo_id
        raise HTTPException(status_code=403, detail="Not enough permissions")
