from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, assert_never

from etl_pipes.context import Context
from etl_pipes.pipes.base_pipe import Pipe
from etl_pipes.pipes.pipeline.pipe_welding_validator import PipeWeldingValidator


@dataclass
class Pipeline(Pipe):
    pipes: list[Pipe] = field(default_factory=list)
    validator: PipeWeldingValidator = field(default_factory=PipeWeldingValidator)
    ignore_validation: bool = field(default=True)
    context: Context | None = field(default=None)

    def __post_init__(self) -> None:
        self.apply_context(self.context)

        self._validate()

    def apply_context(self, context: Context | None) -> None:
        if context:
            for pipe in self.pipes:
                pipe.apply_context(context)

    async def __call__(self, *args: Any) -> Any:
        self._validate()

        data = args

        for pipe in self.pipes:
            data = await self._call_pipe_with_data(pipe, data)

            if pipe.out.is_modified:
                data = self._modify_output(pipe.out.pos, data)
        return data

    async def _call_pipe_with_data(self, pipe: Pipe, data: Any) -> Any:
        # think about library type for this,
        # tuple seems to be a crucial part of the pipeline
        if type(data) is tuple:
            return await pipe(*data)
        else:
            return await pipe(data)

    def _modify_output(self, pos: int | slice | tuple[int, ...], data: Any) -> Any:
        match pos:
            case int():
                data = data[pos]
            case slice():
                data = tuple(data[pos])
            case tuple():
                data = tuple(data[i] for i in pos)
            case _:
                assert_never(pos)
        return data

    def __rshift__(self, other: Pipe) -> Pipeline:
        self.pipes.append(other)
        self._validate()

        return self

    def _validate(self) -> None:
        if self.ignore_validation:
            return
        self.validator.validate(self.pipes)
