from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BasePipe:
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError()

    def __rshift__(self, other: BasePipe) -> Pipeline:
        return Pipeline([self, other])


@dataclass
class Pipeline:
    pipes: list[BasePipe] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._validate()

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._validate()

        first_pipe = self.pipes[0]
        data = await first_pipe(*args, **kwargs)
        for pipe in self.pipes[1:]:
            data = await pipe(data)
        return data

    def __rshift__(self, other: BasePipe) -> Pipeline:
        self.pipes.append(other)
        self._validate()

        return self

    def _validate(self) -> None:
        if not self.pipes:
            raise NoPipesInPipelineError()
        if len(self.pipes) == 1:
            raise OnlyOnePipeInPipelineError()

        for pipe in self.pipes:
            if not isinstance(pipe, BasePipe):
                raise ElementIsNotPipeError(pipe)

        self._validate_pipe_typing()

    def _validate_pipe_typing(self) -> None:
        for i in range(len(self.pipes) - 1):
            next_pipe = self.pipes[i + 1]
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

            current_pipe = self.pipes[i]
            current_pipe_return_type = current_pipe.__call__.__annotations__.get(
                "return"
            )

            if current_pipe_return_type != next_pipe_arg_type:
                raise PipelineTypeError(current_pipe, next_pipe)


class PipelineError(Exception):
    pass


class NoPipesInPipelineError(PipelineError):
    pass


class OnlyOnePipeInPipelineError(PipelineError):
    pass


class ElementIsNotPipeError(PipelineError):
    def __init__(self, element: Any) -> None:
        self.element = element
        super().__init__(f"Element {element} is not a pipe")


class PipelineTypeError(PipelineError):
    def __init__(self, current_pipe: BasePipe, next_pipe: BasePipe) -> None:
        self.current_pipe = current_pipe
        self.next_pipe = next_pipe
        super().__init__(self._generate_message())

    def _generate_message(self) -> str:
        s = "Typing error in pipeline:\n"
        cp_anns = self.current_pipe.__call__.__annotations__
        s += (
            f"{self.current_pipe}: "
            f"{self._transform_annotations_to_readable_string(cp_anns)}\n"
        )
        np_anns = self.next_pipe.__call__.__annotations__
        s += (
            f"{self.next_pipe}: "
            f"{self._transform_annotations_to_readable_string(np_anns)}\n"
        )
        return s

    def _transform_annotations_to_readable_string(
        self, annotations_: dict[str, Any]
    ) -> str:
        s = (
            "(\n\t\t"
            + ", \n".join(
                [
                    f"{key}: {value}"
                    for key, value in annotations_.items()
                    if key != "return"
                ]
            )
            + "\n)"
        )
        s = s + " -> " + str(annotations_.get("return"))
        return s
