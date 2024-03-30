import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from etl_pipes.common.actor import Actor, ActorSystem, Message


@pytest.mark.asyncio
async def test_simple_actor() -> None:
    async def initialize_queue() -> asyncio.Queue[Message]:
        init_queue: asyncio.Queue[Message] = asyncio.Queue()
        for msg in [Message(data="11,22,33"), Message(data="44,55,66")]:
            init_queue.put_nowait(msg)
        return init_queue

    @dataclass
    class SplittingActor(Actor):
        name: str = "splitting_actor"

        async def process_message(self, message_data: Any) -> None:
            for digits in message_data.split(","):
                await self.save_result(digits)

    @dataclass
    class DigitActor(Actor):
        name: str = "digit_actor"

        async def process_message(self, message_data: Any) -> None:
            data = message_data
            if not all(char.isdigit() for char in data):
                await self.save_exception(Exception(f"Non-digit character: {data}"))
            for char in data:
                await self.save_result(int(char))

    @dataclass
    class PrintActor(Actor):
        name: str = "print_actor"

        async def process_message(self, message_data: Any) -> None:
            await self.save_result(str(message_data))

    splitting_actor = SplittingActor(incoming_messages=await initialize_queue())
    digit_actor = DigitActor()
    print_actor = PrintActor()

    actor_system = ActorSystem(
        actors=[splitting_actor, digit_actor, print_actor],
        no_action_timeout=timedelta(seconds=1),
    )

    splitting_actor >> digit_actor >> print_actor

    await actor_system.run()

    results = await actor_system.get_actor_unpacked_results(print_actor)
    assert results == [
        "1",
        "1",
        "2",
        "2",
        "3",
        "3",
        "4",
        "4",
        "5",
        "5",
        "6",
        "6",
    ]

    exceptions = await actor_system.get_actor_unpacked_exceptions(print_actor)
    assert exceptions == []
