from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pytest

from etl_pipes.pipes.map_reduce import MapReduce


@pytest.mark.asyncio
async def test_if_map_reduce_pipe_works() -> None:
    @dataclass
    class SomeMapReduce(MapReduce):
        async def split(self, iterable: Iterable[Any]) -> Iterable[Any]:
            return [*iterable]

        async def map(self, chunks: Iterable[Any]) -> Iterable[Any]:
            def log_chunk(idx: int, c_: Any) -> Any:
                print(f"chunk {idx}: {c_}")
                return c_

            results = [log_chunk(i, chunk) for i, chunk in enumerate(chunks)]
            return results

        async def reduce(self, mapped_chunks: Iterable[Any]) -> Any:
            return tuple(mapped_chunks)

    @dataclass
    class SomeObject:
        a: int = 0
        b: str = ""
        c: float = 0.0

    map_reduce = SomeMapReduce()
    chunks = (1, "string", 2.0, 3, "another string", SomeObject())
    result = await map_reduce(chunks)

    assert result == (1, "string", 2.0, 3, "another string", SomeObject())
