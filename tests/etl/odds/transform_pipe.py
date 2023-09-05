from dataclasses import dataclass

from _decimal import Decimal

from etl_pipes.pipes.pipeline.base_pipe import Pipe
from tests.etl.odds.types import InnerOdds, OddsId, OuterOdds


@dataclass
class OuterToInnerPipe(Pipe):
    async def __call__(self, outer_odds: OuterOdds) -> InnerOdds:
        id_, name = outer_odds.selection.name.split("_")
        value = Decimal(
            round(
                Decimal(outer_odds.fraction.numerator)
                / Decimal(outer_odds.fraction.denominator)
                + Decimal(1),
                2,
            )
        )
        return InnerOdds(
            value=value,
            id=OddsId(id_),
            name=name,
        )
