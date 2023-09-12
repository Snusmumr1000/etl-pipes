import asyncio
from dataclasses import dataclass
from typing import Any

from etl_pipes.pipes.pipeline.pipeline import Pipeline


@dataclass
class Parallel(Pipeline):
    broadcast: bool = False

    async def __call__(self, *args: Any, **kwargs: Any) -> tuple[Any, ...]:
        tasks = []
        if self.broadcast:
            for pipe in self.pipes:
                tasks.append(asyncio.create_task(pipe(*args, **kwargs)))
        else:
            tuple_of_args = args[0]
            for pipe, arg in zip(self.pipes, tuple_of_args):
                tasks.append(asyncio.create_task(pipe(arg)))
        results = await asyncio.gather(*tasks)
        return tuple(results)
