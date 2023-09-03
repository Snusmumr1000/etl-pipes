from decimal import Decimal
from fractions import Fraction

import pytest

from tests.etl.odds.transform_pipe import OuterToInnerPipe
from tests.etl.odds.types import InnerOdds, OddsId, OuterOdds, Selection


@pytest.fixture
def simple_outer_odds() -> OuterOdds:
    return OuterOdds(
        fraction=Fraction(numerator=1, denominator=3),
        selection=Selection(name="123_AsianHandicap0.25"),
    )


@pytest.fixture
def simple_inner_odds() -> InnerOdds:
    return InnerOdds(
        value=Decimal("1.33"),
        id=OddsId("123"),
        name="AsianHandicap0.25",
    )


@pytest.fixture
def simple_outer_to_inner_pipe() -> OuterToInnerPipe:
    return OuterToInnerPipe()
