"""Tests for schema inference."""

import pytest

from polypolars import infer_schema
from polypolars.exceptions import SchemaInferenceError


def test_infer_schema_dataclass(skip_if_no_polars):
    from dataclasses import dataclass

    import polars as pl

    @dataclass
    class User:
        id: int
        name: str
        score: float
        active: bool

    schema = infer_schema(User)
    assert isinstance(schema, dict)
    assert schema["id"] == pl.Int64
    assert schema["name"] == pl.String
    assert schema["score"] == pl.Float64
    assert schema["active"] == pl.Boolean


def test_infer_schema_optional(skip_if_no_polars):
    from dataclasses import dataclass
    from typing import Optional

    import polars as pl

    @dataclass
    class Model:
        required: str
        optional: Optional[int] = None

    schema = infer_schema(Model)
    assert schema["required"] == pl.String
    assert schema["optional"] == pl.Int64


def test_infer_schema_list(skip_if_no_polars):
    from dataclasses import dataclass
    from typing import List

    import polars as pl

    @dataclass
    class Model:
        tags: List[str]
        ids: List[int]

    schema = infer_schema(Model)
    assert schema["tags"] == pl.List(pl.String)
    assert schema["ids"] == pl.List(pl.Int64)


def test_infer_schema_subset_columns(skip_if_no_polars):
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str
        email: str

    schema = infer_schema(User, schema=["id", "name"])
    assert list(schema.keys()) == ["id", "name"]


def test_infer_schema_invalid_column_raises(skip_if_no_polars):
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    with pytest.raises(SchemaInferenceError):
        infer_schema(User, schema=["id", "nonexistent"])


def test_infer_schema_typed_dict(skip_if_no_polars):
    from typing import TypedDict

    import polars as pl

    class MyTypedDict(TypedDict):
        id: int
        name: str
        score: float

    schema = infer_schema(MyTypedDict)
    assert schema["id"] == pl.Int64
    assert schema["name"] == pl.String
    assert schema["score"] == pl.Float64


def test_build_dataframe_from_typed_dict_schema(skip_if_no_polars):
    """TypedDict schema can be used to create a DataFrame from dicts (polyfactory does not build TypedDict)."""
    from typing import TypedDict

    import polars as pl

    from polypolars import infer_schema

    class ItemDict(TypedDict):
        key: int
        value: str

    schema = infer_schema(ItemDict)
    data = [{"key": 1, "value": "a"}, {"key": 2, "value": "b"}]
    df = pl.DataFrame(data, schema=schema)
    assert df.height == 2
    assert list(df.columns) == ["key", "value"]
    assert df["key"].dtype == pl.Int64
    assert df["value"].dtype == pl.String
