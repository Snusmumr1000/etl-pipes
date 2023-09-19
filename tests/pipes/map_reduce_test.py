from dataclasses import dataclass

import pytest

from etl_pipes.pipes.map_reduce import MapReduce


@pytest.mark.asyncio
async def test_if_map_reduce_pipe_works() -> None:
    @dataclass
    class SomeObject:
        a: int = 0
        b: str = ""
        c: float = 0.0

    map_reduce = MapReduce()
    chunks = (1, "string", 2.0, 3, "another string", SomeObject())
    result = await map_reduce(*chunks)

    assert result == (1, "string", 2.0, 3, "another string", SomeObject())
