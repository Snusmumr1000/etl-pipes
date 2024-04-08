from dataclasses import dataclass, fields
from typing import Any


class Context:
    def has_part(self, part_name: str, part_type: type) -> bool:
        # assume all the children of Context are dataclasses
        for dc_field in fields(self):  # type: ignore
            if dc_field.name == part_name and dc_field.type == part_type:
                return True

        return False

    def get_part(self, part_name: str) -> Any:
        # assume all the children of Context are dataclasses
        for dc_field in fields(self):  # type: ignore
            if dc_field.name == part_name:
                return getattr(self, part_name)


@dataclass(frozen=True)
class ContextPart:
    context: type[Context]

    def is_part_of(self, context: type[Context]) -> bool:
        return self.context == context


def full(context: type[Context]) -> Any:
    return context


def single(context: type[Context]) -> Any:
    return ContextPart(context)
