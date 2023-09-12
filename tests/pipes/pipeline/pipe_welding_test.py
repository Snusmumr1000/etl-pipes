from typing import Any

import pytest

from etl_pipes.pipes.pipeline.pipe_welding_validator import is_compatible_type


@pytest.mark.parametrize(
    "value_type, signature_type, expected",
    [
        (int, int, True),
        (int, float, False),
        (int, int | str, True),
        (str, int | str, True),
        (float, int | str, False),
        (type(None), int | None, True),
        (int, int | None, True),
        (float, int | None, False),
        (int, Any, True),
        (str, Any, True),
        (float, Any, True),
        (list[int], list[int], True),
        (list[str], list[int], False),
        (list[int], list[Any], True),
        (dict[str, int], dict[str, int], True),
        (dict[str, int], dict[Any, Any], True),
        (dict[str, int], dict[int, int], False),
    ],
)
def test_is_compatible_type(
    value_type: type, signature_type: type, expected: bool
) -> None:
    result = is_compatible_type(value_type, signature_type)
    assert result == expected, (
        f"Expected {expected} but got {result} "
        f"while testing {value_type} against {signature_type}"
    )
