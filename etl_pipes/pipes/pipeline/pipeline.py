from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from etl_pipes.pipes.pipeline.base_pipe import Pipe
from etl_pipes.pipes.pipeline.pipe_welding_validator import PipeWeldingValidator


@dataclass
class Pipeline(Pipe):
    pipes: list[Pipe] = field(default_factory=list)
    validator: PipeWeldingValidator = field(default_factory=PipeWeldingValidator)

    def __post_init__(self) -> None:
        self._validate()

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._validate()

        first_pipe, *rest_of_pipes = self.pipes
        data = await first_pipe(*args, **kwargs)
        for pipe in rest_of_pipes:
            data = await pipe(data)
        return data

    def __rshift__(self, other: Pipe) -> Pipeline:
        self.pipes.append(other)
        self._validate()

        return self

    def _validate(self) -> None:
        self.validator.validate(self.pipes)
