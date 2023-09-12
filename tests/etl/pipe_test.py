import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from etl_pipes.pipes.base_pipe import Pipe
from etl_pipes.pipes.map_reduce import MapReduce
from etl_pipes.pipes.parallel import Parallel
from etl_pipes.pipes.pipeline.exceptions import PipelineTypeError
from etl_pipes.pipes.pipeline.pipeline import Pipeline
from tests.etl.odds.convert_to_american_pipe import ToAmericanPipe
from tests.etl.odds.transform_pipe import OuterToInnerPipe
from tests.etl.odds.types import InnerOdds, OuterOdds


@pytest.mark.asyncio
async def test_simple_transformation(
    simple_outer_odds: OuterOdds,
    simple_inner_odds: InnerOdds,
    simple_outer_to_inner_pipe: OuterToInnerPipe,
) -> None:
    converted_data = await simple_outer_to_inner_pipe(simple_outer_odds)

    assert converted_data == simple_inner_odds


@pytest.mark.asyncio
async def test_simple_two_step_transformation(
    simple_outer_odds: OuterOdds,
    simple_inner_american_odds: InnerOdds,
    simple_inner_odds: InnerOdds,
    simple_outer_to_inner_pipe: OuterToInnerPipe,
    convert_to_american_pipe: ToAmericanPipe,
) -> None:
    converted_data = await simple_outer_to_inner_pipe(simple_outer_odds)
    converted_data = await convert_to_american_pipe(converted_data)

    assert converted_data == simple_inner_american_odds


@pytest.mark.asyncio
async def test_simple_combination_of_transformations(
    simple_outer_odds: OuterOdds,
    simple_inner_american_odds: InnerOdds,
    simple_inner_odds: InnerOdds,
    simple_outer_to_inner_pipe: OuterToInnerPipe,
    convert_to_american_pipe: ToAmericanPipe,
) -> None:
    pipeline = Pipeline(
        [
            simple_outer_to_inner_pipe,
            convert_to_american_pipe,
        ]
    )

    converted_data = await pipeline(simple_outer_odds)

    assert converted_data == simple_inner_american_odds


@pytest.mark.asyncio
async def test_cant_connect_unmatching_pipes(
    simple_outer_to_inner_pipe: OuterToInnerPipe,
) -> None:
    with pytest.raises(PipelineTypeError):
        Pipeline(
            [
                simple_outer_to_inner_pipe,
                simple_outer_to_inner_pipe,
            ]
        )


@pytest.mark.asyncio
async def test_parallel_log_to_console_and_log_to_file() -> None:
    class LogToConsolePipe(Pipe):
        async def __call__(self, data: str) -> int:
            await asyncio.sleep(0.5)
            print(data)
            return 1

    test_file = Path("/tmp/pipe-test.txt")
    if test_file.exists():
        test_file.unlink()

    class LogToFilePipe(Pipe):
        async def __call__(self, data: str) -> str:
            with test_file.open("a") as f:
                f.write(data)
            await asyncio.sleep(0.5)
            return "file"

    log_to_console_pipe = LogToConsolePipe()
    log_to_file_pipe = LogToFilePipe()

    start_time_ms = int(time.time() * 1000)
    parallel = Parallel(
        [
            log_to_console_pipe,
            log_to_file_pipe,
        ],
        broadcast=True,
    )

    results = await parallel("test")

    assert results == (1, "file")

    end_time_ms = int(time.time() * 1000)

    limit = 650
    diff = end_time_ms - start_time_ms
    assert diff < limit

    assert test_file.exists()
    assert test_file.read_text() == "test"


@pytest.mark.asyncio
async def test_if_as_base_pipe_works() -> None:
    @Pipe.as_pipe
    def sum_(a: int, b: int) -> int:
        return a + b

    @Pipe.as_pipe
    def pow_(a: int) -> int:
        r = 1
        for _ in range(a):
            r *= a
        return r

    pipeline = Pipeline([sum_, pow_])

    result = await pipeline(2, 2)

    assert result == (2 + 2) ** (2 + 2)


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
