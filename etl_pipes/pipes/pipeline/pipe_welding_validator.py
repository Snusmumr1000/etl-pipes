import inspect
from dataclasses import dataclass
from types import UnionType
from typing import Any, Union, get_args, get_origin

from etl_pipes.pipes.base_pipe import Pipe
from etl_pipes.pipes.pipeline.exceptions import (
    ElementIsNotPipeError,
    NoPipesInPipelineError,
    OnlyOnePipeInPipelineError,
    PipelineTypeError,
)


def is_compatible_type(value_type: type, signature_type: type) -> bool:
    union_origins = {Union, UnionType}  # UnionType is for Python 3.10+

    # If the signature allows Any type, then any value is acceptable
    if signature_type is Any:
        return True

    # TODO: remove and fix Parallel class
    #       to change the signature of the call method when initialized
    if value_type == tuple[Any, ...]:
        return True

    # Check if the value_type is compatible with any of the Union's elements
    if get_origin(signature_type) in union_origins:
        return any(is_compatible_type(value_type, t) for t in get_args(signature_type))

    # If the signature specifies Optional[T], it's the same as Union[T, None]
    if get_origin(signature_type) in union_origins and type(None) in get_args(
        signature_type
    ):
        return value_type is type(None) or any(
            is_compatible_type(value_type, t)
            for t in get_args(signature_type)
            if t is not type(None)
        )

    # For generic types like List, Dict, etc.
    if get_origin(signature_type) is not None:
        # Check if the origins match (e.g., List, Dict)
        # TODO: do we want list and List to be compatible?
        if get_origin(value_type) is not get_origin(signature_type):
            return False

        # Check type arguments (e.g., the T in List[T])
        for value_arg, signature_arg in zip(
            get_args(value_type), get_args(signature_type)
        ):
            if not is_compatible_type(value_arg, signature_arg):
                return False
        return True

    # For basic types, check if the value_type is a subclass of the signature_type
    return issubclass(value_type, signature_type)


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
            current_pipe = pipes[i]
            if current_pipe.is_void:
                continue

            next_pipe = pipes[i + 1]

            current_pipe_call = current_pipe.get_callable()
            next_pipe_call = next_pipe.get_callable()

            # Using inspect to get annotations
            next_pipe_signature = inspect.signature(next_pipe_call)
            next_pipe_arg_types: list[type] = [
                param.annotation
                for name, param in next_pipe_signature.parameters.items()
            ]

            # Handle cases where there are no annotations or multiple annotations
            next_pipe_arg_type: type = type(None)
            match len(next_pipe_arg_types):
                case 0:
                    pass
                case 1:
                    next_pipe_arg_type = next_pipe_arg_types[0]
                case _:
                    next_pipe_arg_type = tuple[*next_pipe_arg_types]  # type: ignore

            # Get the return type annotation for the current pipe
            current_pipe_return_type = inspect.signature(
                current_pipe_call
            ).return_annotation

            # Use is_compatible_type to check for type compatibility

            if not is_compatible_type(current_pipe_return_type, next_pipe_arg_type):
                raise PipelineTypeError(current_pipe, next_pipe)
