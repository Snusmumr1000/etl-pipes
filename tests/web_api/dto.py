from pydantic import BaseModel, RootModel

from tests.web_api.domain_types import ItemId, Todo, TodoId


class GetItemDto(BaseModel):
    id: ItemId
    todo_id: TodoId
    name: str
    description: str
    done: bool
    order: int


class CreateOrUpdateItemDto(BaseModel):
    id: ItemId | None = None
    todo_id: TodoId
    name: str
    description: str = ""
    done: bool = False
    order: int = -1


class CreateTodoDto(BaseModel):
    name: str
    description: str = ""


class GetTodoDto(RootModel[Todo]):
    root: Todo
