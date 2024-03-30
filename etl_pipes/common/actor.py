from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Any, NewType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class Message:
    data: Any


ActorId = NewType("ActorId", str)


@dataclass
class Actor:
    name: str
    id: ActorId = field(default_factory=lambda: ActorId(str(uuid.uuid4())))

    incoming_messages: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    incoming_exceptions: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    results_buffer: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    results: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    exceptions_buffer: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    exceptions: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)

    processing_messages_task: asyncio.Task[None] | None = None

    receiving_actors: dict[ActorId, Actor] = field(default_factory=dict)
    sending_actors: dict[ActorId, Actor] = field(default_factory=dict)

    async def process_messages(self) -> None:
        while True:
            message = await self.incoming_messages.get()
            await self.process_message(message.data)

            while not self.results_buffer.empty():
                response = await self.results_buffer.get()
                if response is not None:
                    if self.receiving_actors:
                        for actor in self.receiving_actors.values():
                            await actor.accept_result_message(response)
                        continue
                    await self.results.put(response)

            while not self.exceptions_buffer.empty():
                exception = await self.exceptions_buffer.get()
                if exception is not None:
                    if self.receiving_actors:
                        for actor in self.receiving_actors.values():
                            await actor.accept_exception_message(exception)
                            continue
                    await self.exceptions.put(exception)

    async def accept_result_message(self, message: Message) -> None:
        await self.incoming_messages.put(message)

    async def accept_exception_message(self, exception_message: Message) -> None:
        await self.incoming_exceptions.put(exception_message)

    async def save_result(self, result: Any) -> None:
        await self.results_buffer.put(Message(data=result))

    async def save_exception(self, exception: Exception) -> None:
        await self.exceptions_buffer.put(Message(data=exception))

    async def process_message(self, message: Message) -> None:
        raise NotImplementedError("Actor must implement process_message method")

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


class OutputType(Enum):
    RESULT = "result"
    EXCEPTION = "exception"


@dataclass
class ActorSystem:
    actors: list[Actor]
    no_action_timeout: timedelta
    no_outcome_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=10))

    actors_dict: dict[ActorId, Actor] = field(default_factory=dict)
    actor_ids: set[ActorId] = field(default_factory=set)
    resulting_actor_ids: set[ActorId] = field(default_factory=set)
    starting_actor_ids: set[ActorId] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.actors_dict = {actor.id: actor for actor in self.actors}
        self.actor_ids = {actor.id for actor in self.actors}

    async def run(self) -> None:
        # TBD: validation, so non-starting actors don't have any messages in inbox
        self.categorize_actors()
        non_starting_actor_ids = self.actor_ids - self.starting_actor_ids

        tasks = {}

        for actor_id in self.starting_actor_ids:
            actor = self.actors_dict[actor_id]
            tasks[actor_id] = asyncio.create_task(actor.process_messages())

        for actor_id in non_starting_actor_ids:
            actor = self.actors_dict[actor_id]
            tasks[actor_id] = asyncio.create_task(actor.process_messages())

        # TBD: system of ticks while processing messages
        await asyncio.wait(
            [*tasks.values()], timeout=self.no_action_timeout.total_seconds()
        )

        for task in tasks.values():
            task.cancel()

    def categorize_actors(self) -> None:
        for actor_id, actor in self.actors_dict.items():
            if not actor.sending_actors:
                self.starting_actor_ids.add(actor_id)
            if not actor.receiving_actors:
                self.resulting_actor_ids.add(actor_id)

    async def stream_actor_unpacked_results(
        self, actor: Actor, timeout: timedelta | None = None
    ) -> AsyncGenerator[Any, None]:
        async for message in self.stream_actor_output_with_timeout(
            actor, OutputType.RESULT, timeout
        ):
            yield message.data

    async def stream_actor_unpacked_exceptions(
        self, actor: Actor, timeout: timedelta | None = None
    ) -> AsyncGenerator[Exception, None]:
        async for message in self.stream_actor_output_with_timeout(
            actor, OutputType.EXCEPTION, timeout
        ):
            yield message.data

    async def stream_actor_output_with_timeout(
        self, actor: Actor, output_type: OutputType, timeout: timedelta | None = None
    ) -> AsyncGenerator[Message, None]:
        if timeout is None:
            timeout = self.no_outcome_timeout
        async for message in self.stream_actor_output(actor, output_type, timeout):
            yield message

    async def stream_actor_output(
        self, actor: Actor, output_type: OutputType, timeout: timedelta
    ) -> AsyncGenerator[Message, None]:
        queue: asyncio.Queue[Message] = asyncio.Queue()
        match output_type:
            case OutputType.RESULT:
                queue = self.get_results()[actor.id][0]
            case OutputType.EXCEPTION:
                queue = self.get_results()[actor.id][1]

        while True:
            try:
                message = await asyncio.wait_for(
                    queue.get(), timeout=timeout.total_seconds()
                )
                yield message
            except TimeoutError:
                break

    def get_results(
        self,
    ) -> dict[ActorId, tuple[asyncio.Queue[Message], asyncio.Queue[Message]]]:
        results = {}
        for actor_id in self.resulting_actor_ids:
            actor = self.actors_dict[actor_id]
            results[actor_id] = (actor.results, actor.exceptions)
        return results
