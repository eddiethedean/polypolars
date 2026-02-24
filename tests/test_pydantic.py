"""Tests for Pydantic model support."""

import pytest

pytest.importorskip("polars")
pytest.importorskip("pydantic")

from typing import Optional

import polars as pl
from pydantic import BaseModel, Field

from polypolars import infer_schema, polars_factory


def test_pydantic_build_dataframe():
    @polars_factory
    class User(BaseModel):
        id: int = Field(gt=0)
        username: str = Field(min_length=1, max_length=50)
        email: str
        is_active: bool = True

    df = User.build_dataframe(size=20)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 20
    assert list(df.columns) == ["id", "username", "email", "is_active"]
    assert df["id"].dtype == pl.Int64
    assert df["is_active"].dtype == pl.Boolean


def test_pydantic_optional():
    @polars_factory
    class Model(BaseModel):
        required: str
        optional: Optional[int] = None

    schema = infer_schema(Model)
    assert schema["required"] == pl.String
    assert schema["optional"] == pl.Int64

    df = Model.build_dataframe(size=5)
    assert df.height == 5


def test_pydantic_build_dicts():
    @polars_factory
    class Item(BaseModel):
        name: str
        value: float

    dicts = Item.build_dicts(size=3)
    assert len(dicts) == 3
    for d in dicts:
        assert "name" in d and "value" in d
        assert isinstance(d["value"], (int, float))
