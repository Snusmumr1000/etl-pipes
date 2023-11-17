from dataclasses import dataclass

from fastapi import HTTPException

from etl_pipes.pipes.base_pipe import Pipe
from tests.web_api.dto import CreateOrUpdateItemDto, CreateTodoDto


@dataclass
class ValidateTodo(Pipe):
    async def __call__(self, todo: CreateTodoDto) -> CreateTodoDto:
        if todo.name == "":
            raise HTTPException(status_code=400, detail="Title cannot be empty")
        return todo


@dataclass
class ValidateItem(Pipe):
    item: CreateOrUpdateItemDto

    async def __call__(self) -> CreateOrUpdateItemDto:
        if self.item.name == "":
            raise HTTPException(status_code=400, detail="Item name cannot be empty")
        return self.item
