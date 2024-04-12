from typing import Any

from etl_pipes.pipes.base_pipe import Pipe


class Void(Pipe):
    async def __call__(self, *args: Any) -> tuple[()]:
        return ()
