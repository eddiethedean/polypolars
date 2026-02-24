"""Tests for nested structs and complex types."""

import pytest

pytest.importorskip("polars")

from dataclasses import dataclass
from typing import Dict, List

import polars as pl

from polypolars import infer_schema, polars_factory


def test_nested_dataclass():
    @dataclass
    class Address:
        street: str
        city: str
        zipcode: str

    @polars_factory
    @dataclass
    class Person:
        id: int
        name: str
        address: Address

    schema = infer_schema(Person)
    assert "address" in schema
    assert schema["address"] == pl.Struct(
        [pl.Field("street", pl.String), pl.Field("city", pl.String), pl.Field("zipcode", pl.String)]
    )

    df = Person.build_dataframe(size=5)
    assert df.height == 5
    assert "address" in df.columns
    assert df["address"].dtype == pl.Struct


def test_list_of_str():
    @polars_factory
    @dataclass
    class WithTags:
        id: int
        tags: List[str]

    df = WithTags.build_dataframe(size=3)
    assert df.height == 3
    assert df["tags"].dtype == pl.List(pl.String)


def test_dict_column_schema():
    @dataclass
    class WithDict:
        id: int
        attrs: Dict[str, float]

    schema = infer_schema(WithDict)
    assert "attrs" in schema
    assert schema["attrs"] == pl.List(
        pl.Struct([pl.Field("key", pl.String), pl.Field("value", pl.Float64)])
    )
