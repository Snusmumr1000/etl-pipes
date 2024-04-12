# Etl-Pipes

## Description

etl-pipes is a Python library for creating flow-based programming pipelines.

## Primitives

### Implemented

- Pipeline
  - used to combine pipes into a single sequential pipeline, used to implement "|" operator
- Parallel
  - used to combine pipes into a single parallel pipeline
- Maybe
  - used to implement a pattern Chain of Responsibility and "||" operator
- MapReduce
  - used to implement MapReduce pattern (chunking, mapping chunks, reducing to a single result) 
- Void
  - used to implement ";" operator
- Actor System
  - used to create asynchronous pipelines, using asyncio under the hood
- Context
  - used to pass data in the whole pipeline, read-only


## Examples

### Basic usage

#### Hello world

A trivial example of a pipeline with two simple pipes. 
Here two operations of summing and raising to a power are combined into one pipeline. 
The result of the first pipe is passed to the second pipe as an argument.

```python
import pytest

from etl_pipes.pipes.base_pipe import as_pipe
from etl_pipes.pipes.pipeline.pipeline import Pipeline

@pytest.mark.asyncio
async def test_if_as_base_pipe_works() -> None:
    @as_pipe
    def sum_(a: int, b: int) -> int:
        return a + b

    @as_pipe
    def pow_(a: int) -> int:
        r = 1
        for _ in range(a):
            r *= a
        return r

    pipeline = Pipeline([sum_, pow_])

    result = await pipeline(2, 2)

    assert result == (2 + 2) ** (2 + 2)
```

#### Parallel

An example of a parallel execution of pipes. 
Currently, it works only with asyncio.

```python
import asyncio
import time
from pathlib import Path

import pytest

from etl_pipes.pipes.base_pipe import as_pipe
from etl_pipes.pipes.parallel import Parallel


@pytest.mark.asyncio
async def test_parallel_log_to_console_and_log_to_file() -> None:
    @as_pipe
    async def log_to_console(data: str) -> int:
        await asyncio.sleep(0.5)
        print(data)
        return 1

    @as_pipe
    async def log_to_file(data: str) -> Path:
        test_file = Path("/tmp/pipe-test.txt")
        if test_file.exists():
            test_file.unlink()

        with test_file.open("a") as f:
            f.write(data)
        await asyncio.sleep(0.5)
        return test_file

    parallel = Parallel(
        [
            log_to_console,
            log_to_file,
        ],
        broadcast=True,  # sends same arguments to all pipes
    )

    start_time_ms = int(time.time() * 1000)

    exit_code, log_path = await parallel("test")

    end_time_ms = int(time.time() * 1000)
    limit = 1000
    diff = end_time_ms - start_time_ms
    assert diff < limit

    assert exit_code == 1
    assert log_path == Path("/tmp/pipe-test.txt")
    assert log_path.exists()
    assert log_path.read_text() == "test"
```

#### Maybe

`Maybe` pipe example. 
It is an implementation of pattern Chain of Responsibility. 

If the current pipe fails with `Nothing` exception,
the next pipe is executed with the same arguments.

If no pipe can handle the exception, `UnhandledNothingError` is raised.

```python
import pytest

from etl_pipes.pipes.base_pipe import as_pipe
from etl_pipes.pipes.maybe import Maybe, Nothing, UnhandledNothingError


@as_pipe
async def successful_pipe() -> str:
    return "Success"


@as_pipe
async def failing_pipe() -> str:
    if True:
        raise Nothing()
    return "Failure"



@pytest.mark.asyncio
async def test_maybe_with_fallback_pipe() -> None:
    maybe_pipe = Maybe(failing_pipe).otherwise(successful_pipe)
    result = await maybe_pipe()
    assert result == "Success"


@pytest.mark.asyncio
async def test_maybe_with_all_failing_pipes() -> None:
    maybe_pipe = Maybe(failing_pipe).otherwise(failing_pipe)
    with pytest.raises(UnhandledNothingError):
        await maybe_pipe()
```

#### MapReduce

`MapReduce` pipe example.

It is an implementation of the MapReduce pattern.
It splits the input data, maps each chunk, and then reduces the results to a single value.

```python
@pytest.mark.asyncio
async def test_if_map_reduce_pipe_works() -> None:
    @dataclass
    class SomeMapReduce(MapReduce):
        async def split(self, iterable: Iterable[Any]) -> Iterable[Any]:
            return [*iterable]

        async def map(self, chunks: Iterable[Any]) -> Iterable[Any]:
            def log_chunk(idx: int, c_: Any) -> Any:
                print(f"chunk {idx}: {c_}")
                return c_

            results = [log_chunk(i, chunk) for i, chunk in enumerate(chunks)]
            return results

        async def reduce(self, mapped_chunks: Iterable[Any]) -> Any:
            return tuple(mapped_chunks)

    @dataclass
    class SomeObject:
        a: int = 0
        b: str = ""
        c: float = 0.0

    map_reduce = SomeMapReduce()
    chunks = (1, "string", 2.0, 3, "another string", SomeObject())
    result = await map_reduce(chunks)

    assert result == (1, "string", 2.0, 3, "another string", SomeObject())
```

#### Void

`Void` pipe example.

It is used to ignore the result of the previous pipe.
Here, we are doing a simple check of the token and raising an exception if it is not correct.
If everything is correct, we go on and get an item. Previous data is ignored.

```python
@pytest.mark.asyncio
async def test_pipeline_void() -> None:
    @as_pipe
    def exchange_token(token: str) -> str:
        return token + " exchanged"

    @as_pipe
    def check_auth(token: str) -> bool:
        required_token = "token exchanged"
        if token != required_token:
            raise Exception("Not authorized")
        return True

    @as_pipe
    def get_item() -> dict[str, str]:
        return {"item": "item"}

    pipeline = Pipeline(
        [
            exchange_token,
            check_auth,
            Void(),
            get_item,
        ]
    )

    auth_token = "token"
    item = await pipeline(auth_token)
    assert item == {"item": "item"}

    wrong_token = "wrong token"
    with pytest.raises(Exception):
        await pipeline(wrong_token)
```


#### Actor System

`ActorSystem` example.

It is an experimental feature that allows creating asynchronous pipelines.
It is based on queues and uses them to pass messages between pipes.
Every `Actor` is a separate task that processes messages from the queue.
It's interface is not that minimalistic as other pipes, the work is still in progress.

```python
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
        await actor_system.insert_result_message(msg, to_actor=splitting_actor.id)

    async for result in actor_system.stream_actor_unpacked_results(print_actor):
        results.append(result)
```

#### Context

`Context` example.

It is used to pass data in the whole pipeline, read-only.

```python
@pytest.mark.asyncio
async def test_simple_context() -> None:
    @dataclass
    class FiniteFieldContext(Context):
        finite_field_order: int

    @dataclass
    class MultiplierPipe(Pipe):
        multiplier: int
        ff_context: FiniteFieldContext = full(FiniteFieldContext)  # noqa: RUF009

        async def __call__(self, data: int) -> int:
            res = data * self.multiplier % self.ff_context.finite_field_order
            print(
                f"{data} * {self.multiplier} "
                f"% {self.ff_context.finite_field_order} = {res}"
            )
            return res

    @dataclass
    class MultiplierPipe2(Pipe):
        multiplier: int
        finite_field_order: int = single(FiniteFieldContext)  # noqa: RUF009

        async def __call__(self, data: int) -> int:
            res = data * self.multiplier % self.finite_field_order
            print(f"{data} * {self.multiplier} % {self.finite_field_order} = {res}")
            return res

    pipeline = Pipeline(
        [
            Pipeline(
                [
                    MultiplierPipe(multiplier=2),
                    Pipeline(
                        [
                            MultiplierPipe2(multiplier=3),
                            MultiplierPipe(multiplier=4),
                        ],
                    ),
                    MultiplierPipe2(multiplier=5),
                ],
                context=FiniteFieldContext(finite_field_order=37),
            ),
            MultiplierPipe2(multiplier=6, finite_field_order=29),
            MultiplierPipe(
                multiplier=4, ff_context=FiniteFieldContext(finite_field_order=23)
            ),
        ],
    )

    pipeline_result = await pipeline(1)
    true_result = 1 * 2 % 37 * 3 % 37 * 4 % 37 * 5 % 37 * 6 % 29 * 4 % 23
    assert true_result == pipeline_result
```

### Advanced usage

More sophisticated example from sample ToDo web application.

We want to get a ToDo item from server.
- we check if we have access to this ToDo item.
- we have access -> we try to get it from cache.
- it is not in cache -> we get it from database and cache it.

```python
from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from etl_pipes.pipes.maybe import Maybe
from etl_pipes.pipes.parallel import Parallel
from etl_pipes.pipes.pipeline.pipeline import Pipeline
from etl_pipes.pipes.void import Void
from tests.web_api.auth import AuthToken, CheckAccessForTodo, Ops
from tests.web_api.cache.todo_cache import CacheTodoDTO, GetTodoAndItemsFromCache
from tests.web_api.db import models
from tests.web_api.db.connection import get_db
from tests.web_api.db.read import ReadManyFromDb, ReadOneFromDb
from tests.web_api.domain_types import TodoId
from tests.web_api.dto import (
    GetTodoDto,
)

from tests.web_api.mapping.todo import MapDbTodoAndDbItemsToDto

app = FastAPI()

@app.get("/todos/{todo_id}")
async def read_todo(
    token: AuthToken, todo_id: TodoId, db: Session = Depends(get_db)
) -> GetTodoDto:
    pipeline = Pipeline(
        [
            CheckAccessForTodo(token, todo_id, ops=[Ops.Read]),  # changes return type to None
            Void(),
            Maybe(
                GetTodoAndItemsFromCache(todo_id=todo_id),
            ).otherwise(
                Pipeline(
                    [
                        Parallel(
                            [
                                ReadOneFromDb(
                                    db=db, model=models.Todo, filter={"id": todo_id}
                                ),
                                ReadManyFromDb(
                                    filter={"todo_id": todo_id},
                                    model=models.Item,
                                    db=db,
                                ),
                            ]
                        ),
                        MapDbTodoAndDbItemsToDto(),
                        CacheTodoDTO(),
                    ]
                )
            ),
        ]
    )
    return await pipeline()  # type: ignore[no-any-return]

```

## Critical features

- [ ] Add support for Pipeline State
- [ ] Add support for Pipeline Context
- [ ] Write a mypy plugin to support type checking for pipes instead of doing it in a runtime
- [ ] Add PassedArgs class

### State
State is a way to pass data between pipes.

It should be a more convenient way to pass data between pipes than passing an argument each time. Something like mutable dependency injection.


### Context
Context is a way to pass data between pipes.

Literally read-only State.


### Type checking
Currently, mypy does not support type checking for pipes, and `Pipeline` or `Parallel` pipes' return type is `Any`.

It causes some problems, when narrowing from `Any` to some type (look at web test case).


### PassedArgs

`PassedArgs` is a class that contains all positional and keyword arguments that will be passed to the next pipe.

Now we use tuple for it, and we can only pass positional arguments. It is not convenient to use it in some cases. 

## Non-critical features

- [ ] Add support for `ProcessPool` and `ThreadPool` for `Parallel` pipe
- [ ] Implement `Parallel` pipe as ABC or Protocol and make `AsyncioParallel` a subclass of it
- [ ] Think about getting rid of square brackets in `Parallel` and `Pipeline` and rename them to `par` and `seq` respectively, overall interface improvement
- [ ] Fix broken endpoints in web app test case
- [X] ~~Create `Void` (or `_`) pipe instead of using `.void()` method~~
- [ ] Create more test cases for `MapReduce` pipe