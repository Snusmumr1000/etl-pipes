import pytest

from etl_pipes.pipes.base_pipe import as_pipe
from etl_pipes.pipes.pipeline.pipeline import Pipeline
from etl_pipes.pipes.void import Void


@pytest.mark.asyncio
async def test_simple_data_structure_change() -> None:
    @as_pipe
    async def get_token_full() -> tuple[str, str, dict[str, str]]:
        return "token", "token_meta", {"info": "info"}

    @as_pipe
    async def verify_token(token: str) -> bool:
        return token == "token"

    @as_pipe
    async def verify_pair(token: str, token_meta: str) -> bool:
        return token == "token" and token_meta == "token_meta"

    @as_pipe
    async def dev_null() -> bool:
        return True

    @as_pipe
    async def verify_token_and_info(token: str, info: dict[str, str]) -> bool:
        return token == "token" and info == {"info": "info"}

    pipeline_single = Pipeline(
        [
            get_token_full[0],
            verify_token,
        ],
    )

    result = await pipeline_single()

    assert result is True

    pipeline_with_sliced_return = Pipeline(
        [
            get_token_full[0:2],
            verify_pair,
        ],
    )

    result = await pipeline_with_sliced_return()

    assert result is True

    pipeline_tuple = Pipeline(
        [
            get_token_full[(0, 2)],
            verify_token_and_info,
        ],
    )

    result = await pipeline_tuple()

    assert result is True

    pipeline_none = Pipeline(
        [
            get_token_full,
            Void(),
            dev_null,
        ],
    )

    result = await pipeline_none()

    assert result is True
