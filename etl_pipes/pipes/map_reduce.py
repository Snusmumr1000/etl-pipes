from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from etl_pipes.pipes.base_pipe import Pipe


@dataclass
class MapReduce(Pipe):
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        chunks = await self.split(*args, **kwargs)
        mapped_chunks = await self.map(chunks)
        reduced_result = await self.reduce(mapped_chunks)
        return reduced_result

    async def split(self, *args: Any, **kwargs: Any) -> Sequence[Any]:
        return [*args]

    async def map(self, chunks: Sequence[Any]) -> Sequence[Any]:
        def log_chunk(idx: int, c_: Any) -> Any:
            print(f"chunk {idx}: {c_}")
            return c_

        results = [log_chunk(i, chunk) for i, chunk in enumerate(chunks)]
        return results

    async def reduce(self, mapped_chunks: Sequence[Any]) -> Any:
        return tuple(mapped_chunks)
