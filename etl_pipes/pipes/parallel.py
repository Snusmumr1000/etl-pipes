import asyncio
from dataclasses import dataclass
from typing import Any

from etl_pipes.pipes.base_pipe import Pipe


@dataclass
class Parallel(Pipe):
    pipes: list[Pipe]

    async def __call__(self, *args: Any) -> tuple[Any, ...]:
        tasks = []
        if not args:
            tasks = self._create_tasks()
        else:
            for pipe, arg in zip(self.pipes, args):
                tasks.append(self._create_task(pipe, arg))
        results = await asyncio.gather(*tasks)
        return tuple(results)

    def _create_tasks(self, *args: Any) -> list[asyncio.Task[Any]]:
        return [self._create_task(pipe, *args) for pipe in self.pipes]

    def _create_task(self, pipe: Pipe, *args: Any) -> asyncio.Task[Any]:
        return asyncio.create_task(pipe(*args))
