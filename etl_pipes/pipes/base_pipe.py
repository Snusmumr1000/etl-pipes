from __future__ import annotations

import copy
import inspect
from dataclasses import dataclass, field
from typing import Any

from etl_pipes.domain.types import AnyFunc


@dataclass
class Pipe:
    is_void: bool = field(init=False, default=False)
    f: AnyFunc | None = field(init=False, default=None)

    __original_func: AnyFunc | None = field(init=False, default=None)

    async def __call__(self, *args: Any) -> Any:
        if self.f is None:
            raise NotImplementedError(
                "Pipe must be initialized with a coroutine function"
            )
        return await self.f(*args)

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

    def void(self) -> Pipe:
        dc = copy.deepcopy(self)
        dc.is_void = True
        return dc


def as_pipe(func: AnyFunc) -> Pipe:
    pipe = Pipe()
    pipe.func = func

    return pipe
