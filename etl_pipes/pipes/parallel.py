import asyncio
from dataclasses import dataclass
from typing import Any

from etl_pipes.pipes.base_pipe import Pipe


@dataclass
class Parallel(Pipe):
    pipes: list[Pipe]
    broadcast: bool = False

    async def __call__(self, *args: Any, **kwargs: Any) -> tuple[Any, ...]:
        tasks = []
        if self.broadcast or not args:
            tasks = self._create_tasks(*args, **kwargs)
        else:
            tuple_of_args = args[0]
            for pipe, arg in zip(self.pipes, tuple_of_args):
                tasks.append(self._create_task(pipe, arg, **kwargs))
        results = await asyncio.gather(*tasks)
        return tuple(results)

    def _create_tasks(self, *args: Any, **kwargs: Any) -> list[asyncio.Task[Any]]:
        return [self._create_task(pipe, *args, **kwargs) for pipe in self.pipes]

    def _create_task(self, pipe: Pipe, *args: Any, **kwargs: Any) -> asyncio.Task[Any]:
        return asyncio.create_task(pipe(*args, **kwargs))
