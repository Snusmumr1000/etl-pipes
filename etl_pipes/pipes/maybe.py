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
        # appends to empty list
        self.append_responsible_pipe(self.input_pipe)

    async def __call__(self, *args: Any) -> Any:
        for pipe in self.responsible_pipes:
            try:
                return await pipe(*args)
            except Nothing:
                pass
        raise UnhandledNothingError("No pipe was able to handle the input")

    def otherwise(self, pipe: Pipe) -> Maybe:
        return self.__or(pipe)

    def __or(self, pipe: Pipe) -> Maybe:
        self.append_responsible_pipe(pipe)
        return self

    def append_responsible_pipe(self, pipe: Pipe) -> None:
        self.responsible_pipes = [*self.responsible_pipes, pipe]
