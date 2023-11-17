# Pypelines

## Description

Pypelines is a Python library for creating flow-based programming pipelines.

## Examples

### Basic usage

Trivial example of a pipeline with two simple pipes.

```python
from etl_pipes.pipes.pipeline.pipeline import Pipeline
from etl_pipes.pipes.base_pipe import as_pipe

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

