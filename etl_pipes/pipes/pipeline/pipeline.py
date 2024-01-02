from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, assert_never

from etl_pipes.pipes.base_pipe import Pipe
from etl_pipes.pipes.pipeline.pipe_welding_validator import PipeWeldingValidator


@dataclass
class Pipeline(Pipe):
    pipes: list[Pipe] = field(default_factory=list)
    validator: PipeWeldingValidator = field(default_factory=PipeWeldingValidator)
    ignore_validation: bool = field(default=False)

    def __post_init__(self) -> None:
        self._validate()

    async def __call__(self, *args: Any) -> Any:
        self._validate()

        data = args
        prev_pipe = None

        for pipe in self.pipes:
            if prev_pipe and prev_pipe.is_void:
                data = await pipe()
            # think about library type for this,
            # tuple seems to be a crucial part of the pipeline
            elif type(data) is tuple:
                data = await pipe(*data)
            else:
                data = await pipe(data)

            if pipe.out.is_modified:
                match pipe.out.pos:
                    case int():
                        data = data[pipe.out.pos]
                    case slice():
                        data = tuple(data[pipe.out.pos])
                    case tuple():
                        data = tuple(data[i] for i in pipe.out.pos)
                    case _:
                        assert_never(pipe.out.pos)

            prev_pipe = pipe
        return data

    def __rshift__(self, other: Pipe) -> Pipeline:
        self.pipes.append(other)
        self._validate()

        return self

    def _validate(self) -> None:
        if self.ignore_validation:
            return
        self.validator.validate(self.pipes)
