import pytest

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
