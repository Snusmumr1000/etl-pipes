from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from etl_pipes.pipes.base_pipe import Pipe


class Nothing(Exception):  # noqa
    pass


class UnhandledNothingError(Exception):
    pass


@dataclass
class Maybe(Pipe):
    input_pipe: Pipe

    responsible_pipes: list[Pipe] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.responsible_pipes = [*self.responsible_pipes, self.input_pipe]

    def __or(self, pipe: Pipe) -> Maybe:
        self.responsible_pipes = [*self.responsible_pipes, pipe]
        return self

    def otherwise(self, pipe: Pipe) -> Maybe:
        return self.__or(pipe)

    async def __call__(self, *args: Any) -> Any:
        for pipe in self.responsible_pipes:
            try:
                return await pipe(*args)
            except Nothing:
                pass
        raise UnhandledNothingError("No pipe was able to handle the input")
