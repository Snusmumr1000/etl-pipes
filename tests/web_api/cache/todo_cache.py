import logging
from dataclasses import dataclass

from etl_pipes.pipes.base_pipe import Pipe
from etl_pipes.pipes.maybe import Nothing
from tests.web_api.cache.cache import Cache
from tests.web_api.domain_types import TodoId
from tests.web_api.dto import GetTodoDto


@dataclass
class TodoCache(Cache[TodoId, GetTodoDto]):
    ...


cache = TodoCache()


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class GetTodoAndItemsFromCache(Pipe):
    todo_id: TodoId

    async def __call__(self) -> GetTodoDto:
        logger.info("Getting todo from cache")
        value = cache.get(self.todo_id)
        if value is None:
            raise Nothing()

        return value


@dataclass
class CacheTodoDTO(Pipe):
    async def __call__(self, todo: GetTodoDto) -> GetTodoDto:
        logger.info("Caching todo")
        cache.set(todo.root.id, todo)
        return todo
