from dataclasses import dataclass

from etl_pipes.pipes.base_pipe import Pipe
from tests.web_api import domain_types
from tests.web_api.db import models
from tests.web_api.domain_types import TodoId
from tests.web_api.dto import CreateTodoDto, GetTodoDto
from tests.web_api.mapping.item import MapDbTodoItemToDomain


@dataclass
class MapDbTodoAndDbItemsToDto(Pipe):
    async def __call__(self, todo: models.Todo, items: list[models.Item]) -> GetTodoDto:
        _todo = domain_types.Todo(
            id=TodoId(str(todo.id)),
            name=str(todo.name),
            description=str(todo.description),
            items=[await MapDbTodoItemToDomain()(item=item) for item in items],
        )
        return GetTodoDto.model_construct(_todo)


@dataclass
class MapCreateTodoDtoToDb(Pipe):
    async def __call__(self, todo: CreateTodoDto) -> models.Todo:
        return models.Todo(
            **todo.model_dump(),
        )
