from __future__ import annotations

from typing import Any

from etl_pipes.common.utils.type_hints import is_tuple_type_hint
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
        args_formatted = (
            "(\n\t\t"
            + ", \n\t\t".join(
                [f"{value}" for key, value in annotations_.items() if key != "return"]
            )
            + "\n)"
        )
        return_ = annotations_.get("return")
        is_return_tuple = is_tuple_type_hint(return_)  # type: ignore
        args = return_.__args__ if is_return_tuple else [return_]  # type: ignore
        return_formatted = "(\n\t\t" + ", \n".join([f"{arg}" for arg in args]) + "\n)"
        return f"{args_formatted} -> {return_formatted}"
