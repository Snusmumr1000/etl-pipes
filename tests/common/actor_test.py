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
        for msg in [Message(data="11,22,3b3"), Message(data="44,55,66")]:
            init_queue.put_nowait(msg)
        return init_queue

    @dataclass
    class SplittingActor(Actor):
        name: str = "splitting_actor"

        async def process_result(self, message_data: Any) -> None:
            for digits in message_data.split(","):
                await self.save_result(digits)

    @dataclass
    class DigitActor(Actor):
        name: str = "digit_actor"

        async def process_result(self, message_data: Any) -> None:
            data = message_data
            for char in data:
                if not char.isdigit():
                    await self.save_exception(
                        Exception(f"Non-digit character {char} in {data}")
                    )
                    continue
                await self.save_result(int(char))

    @dataclass
    class PrintActor(Actor):
        name: str = "print_actor"

        async def process_result(self, message_data: Any) -> None:
            await self.save_result(str(message_data))

    splitting_actor = SplittingActor(incoming_results=await initialize_queue())
    digit_actor = DigitActor()
    print_actor = PrintActor()

    actor_system = ActorSystem(
        actors=[splitting_actor, digit_actor, print_actor],
        no_action_timeout=timedelta(seconds=1),
        no_outcome_timeout=timedelta(seconds=1),
    )

    splitting_actor >> digit_actor >> print_actor

    await actor_system.run()

    results = []
    async for result in actor_system.stream_actor_unpacked_results(print_actor):
        results.append(result)
    assert sorted(results) == [
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

    exceptions = []
    async for exception in actor_system.stream_actor_unpacked_exceptions(print_actor):
        exceptions.append(exception)

    assert [str(exc) for exc in exceptions] == ["Non-digit character b in 3b3"]
