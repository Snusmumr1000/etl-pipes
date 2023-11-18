from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from etl_pipes.pipes.base_pipe import Pipe


@dataclass
class MapReduce(Pipe, ABC):
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        chunks = await self.split(*args, **kwargs)
        mapped_chunks = await self.map(chunks)
        reduced_result = await self.reduce(mapped_chunks)
        return reduced_result

    @abstractmethod
    async def split(self, *args: Any, **kwargs: Any) -> Sequence[Any]:
        ...

    @abstractmethod
    async def map(self, chunks: Sequence[Any]) -> Sequence[Any]:
        ...

    @abstractmethod
    async def reduce(self, mapped_chunks: Sequence[Any]) -> Any:
        ...
