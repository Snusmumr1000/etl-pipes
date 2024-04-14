from __future__ import annotations

import inspect
from dataclasses import dataclass, field, fields
from typing import Any, assert_never

from etl_pipes.context import Context, ContextPart
from etl_pipes.domain.types import AnyFunc


@dataclass
class PipeOutput:
    is_modified: bool = field(default=False)
    pos: int | slice | tuple[int, ...] = field(default_factory=lambda: slice(None))


@dataclass
class Pipe:
    f: AnyFunc | None = field(init=False, default=None)
    out: PipeOutput = field(init=False, default_factory=PipeOutput)

    __original_func: AnyFunc | None = field(init=False, default=None)

    async def __call__(self, *args: Any) -> Any:
        if self.f is None:
            raise NotImplementedError(
                "Pipe must be initialized with a coroutine function"
            )
        return await self.f(*args)

    def apply_context(self, context: Context | None) -> None:
        if context is None:
            return

        context_class = context.__class__
        for dc_field in fields(self):
            f_name, f_type, f_default = dc_field.name, dc_field.type, dc_field.default

            # we check default, because it is being returned by full
            # that's an indicator of applied full context
            if f_type == context_class and f_default == context_class:
                setattr(self, f_name, context)
                return

            # assume that it's context part
            if isinstance(f_default, ContextPart):
                if context.has_part(f_name, f_type):
                    context_part = f_default
                    if context_part.is_part_of(context_class):
                        setattr(self, f_name, context.get_part(f_name))

    @property
    def func(self) -> AnyFunc | None:
        return self.f

    @func.setter
    def func(self, func: AnyFunc) -> None:
        self.__original_func = func

        if not inspect.iscoroutinefunction(func):

            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            self.f = wrapper
            return

        self.f = func

    def get_callable(self) -> AnyFunc:
        return self.__original_func or self.__call__

    def __str__(self) -> str:
        if self.__original_func is None:
            return self.__class__.__name__
        return self.__original_func.__name__

    def __getitem__(self, key: int | slice | tuple[int, ...]) -> Pipe:
        match key:
            case int() | slice() | tuple():
                # need to make a copy,
                # because the original pipe can be stored in the variable
                # and pipes can be modified parallelly
                pipe = self.shallow_copy()
                pipe.out = PipeOutput(is_modified=True, pos=key)
                return pipe
            case _:
                assert_never(key)

    def shallow_copy(self) -> Pipe:
        pipe = Pipe()
        pipe.func = self.func
        pipe.out = self.out
        return pipe


def as_pipe(func: AnyFunc) -> Pipe:
    pipe = Pipe()
    pipe.func = func

    return pipe
