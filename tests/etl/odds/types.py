from dataclasses import dataclass
from fractions import Fraction
from typing import NewType

from _decimal import Decimal


@dataclass
class Selection:
    name: str


@dataclass
class OuterOdds:
    fraction: Fraction
    selection: Selection


OddsId = NewType("OddsId", str)


@dataclass
class InnerOdds:
    value: Decimal
    id: OddsId
    name: str
