import asyncio
import time
from pathlib import Path

import pytest

from etl_pipes.pipes.base_pipe import Pipe
from etl_pipes.pipes.parallel import Parallel


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
