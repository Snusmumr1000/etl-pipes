import pytest

from etl_pipes.pipes.base_pipe import Pipe
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
