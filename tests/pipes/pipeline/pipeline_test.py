import pytest

from etl_pipes.pipes.base_pipe import Pipe
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
        Pipeline(pipes)
