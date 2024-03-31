from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any

from etl_pipes.actors.common.types import ActorId, Output


@dataclass
class Actor:
    name: str
    id: ActorId = field(default_factory=lambda: ActorId(str(uuid.uuid4())))

    results_buffer: asyncio.Queue[Any] = field(default_factory=asyncio.Queue)
    exceptions_buffer: asyncio.Queue[Exception] = field(default_factory=asyncio.Queue)

    receiving_actors: dict[ActorId, Actor] = field(default_factory=dict)
    sending_actors: dict[ActorId, Actor] = field(default_factory=dict)

    async def process_result(self, result: Any) -> Output | None:
        raise NotImplementedError("Actor must implement process_message method")

    async def process_exception(self, exception: Exception) -> Output | None:
        return Output().save_exception(exception)

    async def save_result(self, result: Any) -> None:
        await self.results_buffer.put(result)

    async def save_exception(self, exception: Exception) -> None:
        await self.exceptions_buffer.put(exception)

    async def stream_results_buffer(self) -> AsyncGenerator[Any, None]:
        while True:
            result = await self.results_buffer.get()
            yield result

    async def stream_exceptions_buffer(self) -> AsyncGenerator[Exception, None]:
        while True:
            exception = await self.exceptions_buffer.get()
            yield exception

    def __rshift__(self, other: Actor) -> Actor:
        self.receiving_actors[other.id] = other
        other.sending_actors[self.id] = self
        return other

    def __rrshift__(self, other: Actor) -> Actor:
        other.receiving_actors[self.id] = self
        self.sending_actors[other.id] = other
        return self

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Actor):
            return False
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)
