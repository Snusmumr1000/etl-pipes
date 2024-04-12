import pytest

from etl_pipes.pipes.base_pipe import Pipe, as_pipe
from etl_pipes.pipes.pipeline.exceptions import (
    NoPipesInPipelineError,
    OnlyOnePipeInPipelineError,
    PipelineTypeError,
)
from etl_pipes.pipes.pipeline.pipeline import Pipeline
from tests.etl.odds.convert_to_american_pipe import ToAmericanPipe
from tests.etl.odds.transform_pipe import OuterToInnerPipe
from tests.etl.odds.types import InnerOdds, OuterOdds


@pytest.mark.asyncio
async def test_pipeline_with_transformations(
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
@pytest.mark.parametrize(
    "pipes_key, expected_exception",
    [
        ("double_pipe", PipelineTypeError),
        ("empty_pipe", NoPipesInPipelineError),
        ("single_pipe", OnlyOnePipeInPipelineError),
    ],
)
async def test_pipeline_errors(
    pipes_key: str,
    expected_exception: type[Exception],
    simple_outer_to_inner_pipe: OuterToInnerPipe,
) -> None:
    pipe_map: dict[str, list[Pipe]] = {
        "double_pipe": [simple_outer_to_inner_pipe, simple_outer_to_inner_pipe],
        "empty_pipe": [],
        "single_pipe": [simple_outer_to_inner_pipe],
    }
    pipes: list[Pipe] = pipe_map[pipes_key]

    with pytest.raises(expected_exception):
        Pipeline(pipes, ignore_validation=False)


@pytest.mark.asyncio
async def test_pipeline_void() -> None:
    await test_pipeline_void_common(True)


@pytest.mark.asyncio
async def test_pipeline_void_none_interface() -> None:
    await test_pipeline_void_common(False)


async def test_pipeline_void_common(use_none_interface: bool = True) -> None:
    @as_pipe
    def exchange_token(token: str) -> str:
        return token + " exchanged"

    @as_pipe
    def check_auth(token: str) -> bool:
        required_token = "token exchanged"
        if token != required_token:
            raise Exception("Not authorized")
        return True

    @as_pipe
    def get_item() -> dict[str, str]:
        return {"item": "item"}

    pipeline = Pipeline(
        [
            exchange_token,
            check_auth[None] if use_none_interface else check_auth.void(),
            get_item,
        ]
    )

    # Run the test for a valid token
    auth_token = "token"
    item = await pipeline(auth_token)
    assert item == {"item": "item"}

    # Run the test for an invalid token
    wrong_token = "wrong token"
    with pytest.raises(Exception):
        await pipeline(wrong_token)
