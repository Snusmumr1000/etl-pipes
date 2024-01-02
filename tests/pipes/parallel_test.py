import asyncio
import time
from pathlib import Path

import pytest

from etl_pipes.pipes.base_pipe import as_pipe
from etl_pipes.pipes.broadcast_parallel import BroadcastParallel
from etl_pipes.pipes.parallel import Parallel


@pytest.mark.asyncio
async def test_parallel_log_to_console_and_log_to_file() -> None:
    @as_pipe
    async def log_to_console(data: str) -> int:
        await asyncio.sleep(0.5)
        print(data)
        return 1

    @as_pipe
    async def log_to_file(data: str) -> Path:
        test_file = Path("/tmp/pipe-test.txt")
        if test_file.exists():
            test_file.unlink()

        with test_file.open("a") as f:
            f.write(data)
        await asyncio.sleep(0.5)
        return test_file

    parallel = BroadcastParallel(
        [
            log_to_console,
            log_to_file,
        ],
    )

    start_time_ms = int(time.time() * 1000)

    exit_code, log_path = await parallel("test")

    end_time_ms = int(time.time() * 1000)
    limit = 1000
    diff = end_time_ms - start_time_ms
    assert diff < limit

    assert exit_code == 1
    assert log_path == Path("/tmp/pipe-test.txt")
    assert log_path.exists()
    assert log_path.read_text() == "test"


@pytest.mark.asyncio
async def test_simple_parallel_execution() -> None:
    @as_pipe
    async def add_one(data: int) -> int:
        await asyncio.sleep(0.5)
        return data + 1

    @as_pipe
    async def add_two(data: int) -> int:
        await asyncio.sleep(0.5)
        return data + 2

    parallel = Parallel(
        [
            add_one,
            add_two,
        ],
    )

    start_time_ms = int(time.time() * 1000)

    result = await parallel(1, 2)

    end_time_ms = int(time.time() * 1000)
    limit = 1000
    diff = end_time_ms - start_time_ms
    assert diff < limit

    assert result == (2, 4)


@pytest.mark.asyncio
async def test_parallel_with_no_arguments() -> None:
    @as_pipe
    async def get_one() -> int:
        await asyncio.sleep(0.5)
        return 1

    @as_pipe
    async def get_two() -> int:
        await asyncio.sleep(0.5)
        return 2

    parallel = Parallel(
        [
            get_one,
            get_two,
        ],
    )

    start_time_ms = int(time.time() * 1000)

    result = await parallel()

    end_time_ms = int(time.time() * 1000)
    limit = 1000
    diff = end_time_ms - start_time_ms
    assert diff < limit

    assert result == (1, 2)
