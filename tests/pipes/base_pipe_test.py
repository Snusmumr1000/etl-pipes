import pytest

from etl_pipes.pipes.base_pipe import as_pipe
from etl_pipes.pipes.pipeline.pipeline import Pipeline


@pytest.mark.asyncio
async def test_simple_data_structure_change() -> None:
    @as_pipe
    async def get_token_full() -> tuple[str, str]:
        return "token", "token_meta"

    @as_pipe
    async def verify_token(token: str) -> bool:
        return token == "token"

    @as_pipe
    async def verify_pair(token: str, token_meta: str) -> bool:
        return token == "token" and token_meta == "token_meta"

    @as_pipe
    async def dev_null() -> bool:
        return True

    pipeline_single = Pipeline(
        [
            get_token_full[0],
            verify_token,
        ],
        ignore_validation=True,  # TODO: remove from this test, broken test semantics
    )

    result = await pipeline_single()

    assert result is True

    pipeline_double = Pipeline(
        [
            get_token_full,
            verify_pair,
        ],
    )

    result = await pipeline_double()

    assert result is True

    pipeline_none = Pipeline(
        [
            get_token_full[None],
            dev_null,
        ],
    )

    result = await pipeline_none()

    assert result is True
