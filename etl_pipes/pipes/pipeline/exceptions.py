from __future__ import annotations

import inspect
from typing import Any

from etl_pipes.pipes.base_pipe import Pipe


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
    def __init__(self, current_pipe: Pipe, next_pipe: Pipe) -> None:
        self.current_pipe = current_pipe
        self.next_pipe = next_pipe
        self.current_pipe_call = current_pipe.get_callable()
        self.next_pipe_call = next_pipe.get_callable()
        super().__init__(self._generate_message())

    def _generate_message(self) -> str:
        s = "Typing error in pipeline:\n"
        cp_sig = inspect.signature(self.current_pipe_call)
        s += (
            f"{self.current_pipe}: "
            f"{self._transform_signature_to_readable_string(cp_sig)}\n"
        )

        np_sig = inspect.signature(self.next_pipe_call)
        s += (
            f"{self.next_pipe}: "
            f"{self._transform_signature_to_readable_string(np_sig)}\n"
        )

        return s

    def _transform_signature_to_readable_string(self, sig: inspect.Signature) -> str:
        params = sig.parameters.values()
        args_formatted = (
            "(\n\t\t"
            + ", \n\t\t".join([str(param.annotation) for param in params])
            + "\n)"
        )

        return_ = sig.return_annotation
        return_formatted = f"{return_}"

        return f"{args_formatted} -> {return_formatted}"
