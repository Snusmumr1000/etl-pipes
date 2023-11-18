import pytest

from etl_pipes.pipes.base_pipe import as_pipe
from etl_pipes.pipes.maybe import Maybe, Nothing, UnhandledNothingError


@as_pipe
async def successful_pipe() -> str:
    return "Success"


@as_pipe
async def failing_pipe() -> str:
    if True:
        raise Nothing()
    return "Failure"


@pytest.mark.asyncio
async def test_maybe_with_successful_pipe() -> None:
    maybe_pipe = Maybe(successful_pipe)
    result = await maybe_pipe()
    assert result == "Success"


@pytest.mark.asyncio
async def test_maybe_with_failing_pipe() -> None:
    maybe_pipe = Maybe(failing_pipe)
    with pytest.raises(UnhandledNothingError):
        await maybe_pipe()


@pytest.mark.asyncio
async def test_maybe_with_fallback_pipe() -> None:
    maybe_pipe = Maybe(failing_pipe).otherwise(successful_pipe)
    result = await maybe_pipe()
    assert result == "Success"


@pytest.mark.asyncio
async def test_maybe_with_all_failing_pipes() -> None:
    maybe_pipe = Maybe(failing_pipe).otherwise(failing_pipe)
    with pytest.raises(UnhandledNothingError):
        await maybe_pipe()
