from dataclasses import dataclass
from decimal import Decimal

from tests.etl.odds.types import InnerOdds


@dataclass
class ToAmericanPipe:
    async def __call__(self, odds: InnerOdds) -> InnerOdds:
        return self.convert_to_american(odds)

    def convert_to_american(self, odds: InnerOdds) -> InnerOdds:
        value = odds.value

        american_decimal_border = Decimal(2)
        new_value = (
            self._convert_positive(value)
            if value >= american_decimal_border
            else self._convert_negative(value)
        )
        return InnerOdds(
            value=Decimal(new_value),
            id=odds.id,
            name=odds.name,
        )

    def _convert_negative(self, value: Decimal) -> int:
        return round(-100 / (value - 1))

    def _convert_positive(self, value: Decimal) -> int:
        return round(100 * (value - 1))
