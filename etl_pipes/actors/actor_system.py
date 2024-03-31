from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncGenerator, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from etl_pipes.actors.actor import Actor
from etl_pipes.actors.common.logging import log_message_info
from etl_pipes.actors.common.types import (
    ActorId,
    Message,
    MessageId,
    MessageTraceId,
    Output,
    OutputType,
)


@dataclass
class ActorSystem:
    actors: list[Actor]
    connections: list[tuple[Actor, Actor]] = field(default_factory=list)
    initial_result_messages: list[Message] = field(default_factory=list)
    initial_exception_messages: list[Message] = field(default_factory=list)

    no_outcome_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    should_be_killed_event: asyncio.Event = field(default_factory=asyncio.Event)

    results_to_send: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    exceptions_to_send: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)

    collected_results: dict[ActorId, asyncio.Queue[Message]] = field(
        default_factory=lambda: defaultdict(asyncio.Queue)
    )
    collected_exceptions: dict[ActorId, asyncio.Queue[Message]] = field(
        default_factory=lambda: defaultdict(asyncio.Queue)
    )

    actors_dict: dict[ActorId, Actor] = field(default_factory=dict)
    actor_ids: set[ActorId] = field(default_factory=set)
    resulting_actor_ids: set[ActorId] = field(default_factory=set)
    starting_actor_ids: set[ActorId] = field(default_factory=set)

    process_tasks: dict[MessageId, asyncio.Task[None]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.actors_dict = {actor.id: actor for actor in self.actors}
        self.actor_ids = {actor.id for actor in self.actors}

    async def run(self) -> None:
        self.generate_pairs()
        await self.distribute_initial_messages()

        async def process_message(
            message: Message,
            process_func: Callable[[Actor, Any], Coroutine[None, None, Output | None]],
            collected: dict[ActorId, asyncio.Queue[Message]],
        ) -> None:
            if message.receiver_id is None:
                log_action = (
                    "considered to be saved"
                    if message.sender_id
                    else "considered to be discarded"
                )
                log_message_info(message, log_action, log_data=True)
                if message.sender_id:
                    await collected[message.sender_id].put(message)
                return

            log_message_info(message, "considered to be sent", log_data=True)
            receiver = self.actors_dict[message.receiver_id]
            output = await process_func(receiver, message.data)
            if output is not None:
                await self.distribute_output(message.trace_id, output, receiver)

        async def message_loop(
            queue: asyncio.Queue[Message],
            process_func: Callable[[Actor, Any], Coroutine[None, None, Output | None]],
            collected: dict[ActorId, asyncio.Queue[Message]],
        ) -> None:
            while True:
                message = await queue.get()
                self.process_tasks[message.id] = asyncio.create_task(
                    process_message(message, process_func, collected)
                )

        async def process_result(receiver: Actor, data: Any) -> Output | None:
            return await receiver.process_result(data)

        async def process_exception(receiver: Actor, data: Exception) -> Output | None:
            return await receiver.process_exception(data)

        # Starting the result and exception processing loops
        result_task = asyncio.create_task(
            message_loop(self.results_to_send, process_result, self.collected_results)
        )
        exception_task = asyncio.create_task(
            message_loop(
                self.exceptions_to_send, process_exception, self.collected_exceptions
            )
        )

        await self.should_be_killed_event.wait()

        result_task.cancel()
        exception_task.cancel()

    def generate_pairs(self) -> None:
        pairs = set()
        for sender_actor_id, sender_actor in self.actors_dict.items():
            for receiver_actor_id in sender_actor.receiving_actors:
                pairs.add((sender_actor_id, receiver_actor_id))
        self.connections = [
            (self.actors_dict[sender_actor_id], self.actors_dict[receiver_actor_id])
            for (sender_actor_id, receiver_actor_id) in pairs
        ]

    async def distribute_initial_messages(self) -> None:
        for message in self.initial_result_messages:
            await self.results_to_send.put(message)
        for exception in self.initial_exception_messages:
            await self.exceptions_to_send.put(exception)

    async def distribute_output(
        self, trace_id: MessageTraceId, output: Output, sender: Actor
    ) -> None:
        async def queue_messages(
            data_items: list[Any], queue: asyncio.Queue[Any]
        ) -> None:
            receiver_ids = sender.receiving_actors or [
                None
            ]  # Default to [None] if empty
            for data_item in data_items:
                for receiver_id in receiver_ids:
                    message = Message(
                        data=data_item,
                        receiver_id=receiver_id,
                        sender_id=sender.id,
                        trace_id=trace_id,
                    )
                    await queue.put(message)

        await queue_messages(output.results, self.results_to_send)
        await queue_messages(output.exceptions, self.exceptions_to_send)

    def kill(self) -> None:
        self.should_be_killed_event.set()

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
                queue = self.get_collected_outputs()[actor.id][0]
            case OutputType.EXCEPTION:
                queue = self.get_collected_outputs()[actor.id][1]

        while True:
            try:
                message = await asyncio.wait_for(
                    queue.get(), timeout=timeout.total_seconds()
                )
                yield message
            except TimeoutError:
                break

    def get_collected_outputs(
        self,
    ) -> dict[ActorId, tuple[asyncio.Queue[Message], asyncio.Queue[Message]]]:
        outputs: dict[
            ActorId, tuple[asyncio.Queue[Message], asyncio.Queue[Message]]
        ] = {}
        for actor_id, collected_results in self.collected_results.items():
            outputs[actor_id] = (collected_results, asyncio.Queue())
        for actor_id, collected_exceptions in self.collected_exceptions.items():
            collected_results, _ = outputs[actor_id]
            outputs[actor_id] = (collected_results, collected_exceptions)
        return outputs
