import asyncio
from dataclasses import dataclass
from typing import Any

from etl_pipes.pipes.parallel import Parallel


@dataclass
class BroadcastParallel(Parallel):
    async def __call__(self, *args: Any) -> tuple[Any, ...]:
        tasks = self._create_tasks(*args)
        results = await asyncio.gather(*tasks)
        return tuple(results)
