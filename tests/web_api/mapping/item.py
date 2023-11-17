from dataclasses import dataclass

from etl_pipes.pipes.base_pipe import Pipe
from tests.web_api import domain_types
from tests.web_api.db import models
from tests.web_api.dto import CreateTodoDto, GetItemDto


@dataclass
class MapDbItemToDto(Pipe):
    async def __call__(self, item: models.Item) -> GetItemDto:
        return GetItemDto.model_construct(item)


@dataclass
class MapDbTodoItemToDomain(Pipe):
    async def __call__(self, item: models.Item) -> domain_types.Item:
        return domain_types.Item(
            id=domain_types.ItemId(str(item.id)),
            name=str(item.name),
            description=str(item.description),
            done=bool(item.done),
            order=int(item.order),
        )


@dataclass
class MapDomainTodoItemToDb(Pipe):
    async def __call__(self, item: domain_types.Item) -> models.Item:
        return models.Item(
            id=item.id,
            name=item.name,
            description=item.description,
            done=item.done,
            order=item.order,
        )


@dataclass
class MapCreateOrUpdateItemDtoToDb(Pipe):
    async def __call__(self, item_dto: CreateTodoDto) -> models.Item:
        return models.Item(
            **item_dto.model_dump(),
        )
