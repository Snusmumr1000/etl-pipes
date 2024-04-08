from dataclasses import dataclass

import pytest

from etl_pipes.context import Context, full, single
from etl_pipes.pipes.base_pipe import Pipe
from etl_pipes.pipes.pipeline.pipeline import Pipeline


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
                ignore_validation=True,
            ),
            MultiplierPipe2(multiplier=6, finite_field_order=29),
            MultiplierPipe(
                multiplier=4, ff_context=FiniteFieldContext(finite_field_order=23)
            ),
        ],
        ignore_validation=True,
    )

    pipeline_result = await pipeline(1)
    true_result = 1 * 2 % 37 * 3 % 37 * 4 % 37 * 5 % 37 * 6 % 29 * 4 % 23
    assert true_result == pipeline_result
