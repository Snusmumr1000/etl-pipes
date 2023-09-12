from dataclasses import dataclass

from etl_pipes.pipes.base_pipe import Pipe
from etl_pipes.pipes.pipeline.exceptions import (
    ElementIsNotPipeError,
    NoPipesInPipelineError,
    OnlyOnePipeInPipelineError,
    PipelineTypeError,
)


@dataclass
class PipeWeldingValidator:
    def validate(self, pipes: list[Pipe]) -> None:
        if not pipes:
            raise NoPipesInPipelineError()
        if len(pipes) == 1:
            raise OnlyOnePipeInPipelineError()

        for pipe in pipes:
            if not isinstance(pipe, Pipe):
                raise ElementIsNotPipeError(pipe)

        self._validate_pipe_typing(pipes)

    def _validate_pipe_typing(self, pipes: list[Pipe]) -> None:
        for i in range(len(pipes) - 1):
            next_pipe = pipes[i + 1]
            next_pipe_types = {**next_pipe.__call__.__annotations__}
            del next_pipe_types["return"]
            next_pipe_arg_types = next_pipe_types

            if len(next_pipe_arg_types.items()) == 0:
                next_pipe_arg_type = None

            elif len(next_pipe_arg_types.items()) == 1:
                next_pipe_arg_type = next_pipe_arg_types.popitem()[1]

            else:
                next_pipe_arg_type = tuple[  # type: ignore
                    *next_pipe_arg_types.values()
                ]

            current_pipe = pipes[i]
            current_pipe_return_type = current_pipe.__call__.__annotations__.get(
                "return"
            )

            if current_pipe_return_type != next_pipe_arg_type:
                raise PipelineTypeError(current_pipe, next_pipe)
