import asyncio
import time
from pathlib import Path

import pytest

from etl_pipes.pipes.base_pipe import as_pipe
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

    parallel = Parallel(
        [
            log_to_console,
            log_to_file,
        ],
        broadcast=True,
    )

    start_time_ms = int(time.time() * 1000)

    exit_code, log_path = await parallel("test")

    end_time_ms = int(time.time() * 1000)
    limit = 650
    diff = end_time_ms - start_time_ms
    assert diff < limit

    assert exit_code == 1
    assert log_path == Path("/tmp/pipe-test.txt")
    assert log_path.exists()
    assert log_path.read_text() == "test"
