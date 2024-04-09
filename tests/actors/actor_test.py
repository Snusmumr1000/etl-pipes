import asyncio
import random
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from etl_pipes.actors.actor import Actor
from etl_pipes.actors.actor_system import ActorSystem
from etl_pipes.actors.common.types import Output


async def random_sleep() -> None:
    await asyncio.sleep(random.uniform(0.1, 0.5))


@dataclass
class SplittingActor(Actor):
    name: str = "splitting_actor"

    async def process_result(self, message_data: Any) -> Output | None:
        await random_sleep()
        output = Output()
        for digits in message_data.split(","):
            output.save_result(digits)
        return output


@dataclass
class DigitActor(Actor):
    name: str = "digit_actor"

    async def process_result(self, message_data: Any) -> Output | None:
        await random_sleep()
        output = Output()
        for char in message_data:
            if not char.isdigit():
                output.save_exception(
                    Exception(f"Non-digit character {char} in {message_data}")
                )
                continue
            output.save_result(int(char))
        return output


@dataclass
class PrintActor(Actor):
    name: str = "print_actor"

    async def process_result(self, message_data: Any) -> Output | None:
        await random_sleep()
        output = Output()
        output.save_result(str(message_data))
        return output


@pytest.mark.asyncio
async def test_simple_actor() -> None:
    splitting_actor = SplittingActor()
    digit_actor = DigitActor()
    print_actor = PrintActor()

    actor_system = ActorSystem(
        actors=[splitting_actor, digit_actor, print_actor],
        no_outcome_timeout=timedelta(seconds=1),
    )

    splitting_actor >> digit_actor >> print_actor

    actor_system_run_task = asyncio.create_task(actor_system.run())
    for msg in ["11,22,3b3", "44,55,66"]:
        await actor_system.insert_result_message(msg, splitting_actor.id)
    await asyncio.sleep(5)
    actor_system.kill()
    await actor_system_run_task

    results = []
    async for result in actor_system.stream_actor_unpacked_results(print_actor):
        results.append(result)
    print()
    print(results)
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

    assert sorted([str(exc) for exc in exceptions]) == ["Non-digit character b in 3b3"]


if __name__ == "__main__":
    asyncio.run(test_simple_actor())
