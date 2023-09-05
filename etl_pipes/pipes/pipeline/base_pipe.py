from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from types import FunctionType, MethodType
from typing import Any

AnyFunc = Callable[..., Any]


@dataclass
class Pipe:
    f: AnyFunc | None = field(init=False, default=None)

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self.f is None:
            raise NotImplementedError(
                "Pipe must be initialized with a coroutine function"
            )
        return await self.f(*args, **kwargs)

    @property
    def func(self) -> AnyFunc | None:
        return self.f

    @func.setter
    def func(self, func: AnyFunc) -> None:
        if not inspect.iscoroutinefunction(func):

            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            self.f = wrapper
            return

        self.f = func

    @staticmethod
    def as_pipe(func: Callable[..., Any]) -> Pipe:
        sig = inspect.signature(func)
        for name, param in sig.parameters.items():
            if param.annotation is inspect.Parameter.empty:
                raise TypeError(
                    f"Function {func.__name__} must have type hints for all parameters"
                )

        pipe = Pipe()

        new_func = FunctionType(
            pipe.__call__.__code__,
            pipe.__call__.__globals__,
            pipe.__call__.__name__,
            pipe.__call__.__defaults__,
            pipe.__call__.__closure__,
        )
        new_annotations = {p.name: p.annotation for p in sig.parameters.values()} | {
            "return": sig.return_annotation
        }
        new_func.__annotations__ = new_annotations
        setattr(pipe, "__call__", MethodType(new_func, func))

        pipe.func = func

        return pipe
