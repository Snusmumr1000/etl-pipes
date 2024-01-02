from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from etl_pipes.pipes.base_pipe import Pipe


@dataclass
class MapReduce(Pipe, ABC):
    async def __call__(self, iterable: Iterable[Any]) -> Any:
        chunks = await self.split(iterable)
        mapped_chunks = await self.map(chunks)
        reduced_result = await self.reduce(mapped_chunks)
        return reduced_result

    @abstractmethod
    async def split(self, iterable: Iterable[Any]) -> Iterable[Any]:
        ...

    @abstractmethod
    async def map(self, chunks: Iterable[Any]) -> Iterable[Any]:
        ...

    @abstractmethod
    async def reduce(self, mapped_chunks: Iterable[Any]) -> Any:
        ...
