import asyncio
from collections.abc import Generator

import pytest

from tests.etl.odds.fixtures.primitive import (  # noqa: F401
    simple_inner_odds,
    simple_outer_odds,
    simple_outer_to_inner_pipe,
)


@pytest.fixture
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for each test case."""
    event_loop = asyncio.get_event_loop_policy().new_event_loop()
    yield event_loop
    event_loop.close()
