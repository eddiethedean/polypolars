"""Edge cases and error paths for schema inference."""

import pytest

pytest.importorskip("polars")

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Tuple

import polars as pl

from polypolars import infer_schema
from polypolars.exceptions import SchemaInferenceError, UnsupportedTypeError


def test_literal_str():
    @dataclass
    class M:
        tag: Literal["a", "b", "c"]

    schema = infer_schema(M)
    assert schema["tag"] == pl.String


def test_literal_int():
    @dataclass
    class M:
        code: Literal[1, 2, 3]

    schema = infer_schema(M)
    assert schema["code"] == pl.Int64


def test_datetime_and_date():
    @dataclass
    class M:
        ts: datetime
        d: date

    schema = infer_schema(M)
    assert "ts" in schema
    assert "d" in schema
    assert schema["d"] == pl.Date


def test_decimal():
    @dataclass
    class M:
        amount: Decimal

    schema = infer_schema(M)
    assert "amount" in schema
    assert schema["amount"] == pl.Decimal(scale=0, precision=10)


def test_empty_list_type_raises():
    @dataclass
    class M:
        items: list  # no inner type

    with pytest.raises((SchemaInferenceError, UnsupportedTypeError)):
        infer_schema(M)


def test_tuple_fixed_to_array():
    @dataclass
    class M:
        coords: Tuple[float, float, float]

    schema = infer_schema(M)
    assert "coords" in schema
    assert schema["coords"] == pl.Array(pl.Float64, 3)


def test_union_optional_picks_first_non_none_type():
    """Ensure Union[T1, T2, None] is treated as optional and unwraps deterministically."""

    from typing import Union

    @dataclass
    class M:
        x: Union[int, str, None]

    schema = infer_schema(M)
    assert schema["x"] == pl.Int64


def test_unsupported_type_raises():
    class Custom:
        pass

    @dataclass
    class M:
        x: Custom

    with pytest.raises(UnsupportedTypeError) as exc_info:
        infer_schema(M)
    assert "Custom" in str(exc_info.value) or "unsupported" in str(exc_info.value).lower()
