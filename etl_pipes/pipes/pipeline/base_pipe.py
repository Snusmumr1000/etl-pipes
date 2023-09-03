from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class BasePipe:
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError()
