"""Tests for polypolars factory."""

import pytest

from polypolars import (
    PolarsFactory,
    build_polars_dataframe,
    polars_factory,
)
from polypolars.exceptions import PolarsNotAvailableError
from polypolars.protocols import is_polars_available


def test_is_polars_available_when_installed():
    """is_polars_available returns True when polars is installed."""
    pytest.importorskip("polars")
    assert is_polars_available() is True


def test_build_dicts_without_polars():
    """build_dicts returns list of dicts."""
    from dataclasses import dataclass

    @polars_factory
    @dataclass
    class User:
        id: int
        name: str

    dicts = User.build_dicts(size=5)
    assert len(dicts) == 5
    for d in dicts:
        assert "id" in d
        assert "name" in d
        assert isinstance(d["id"], int)
        assert isinstance(d["name"], str)


def test_build_dataframe_dataclass(skip_if_no_polars):
    from dataclasses import dataclass

    import polars as pl

    @polars_factory
    @dataclass
    class User:
        id: int
        name: str
        score: float

    df = User.build_dataframe(size=10)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 10
    assert df.columns == ["id", "name", "score"]
    assert df["id"].dtype == pl.Int64
    assert df["name"].dtype == pl.String
    assert df["score"].dtype == pl.Float64


def test_build_dataframe_convenience(skip_if_no_polars):
    from dataclasses import dataclass

    import polars as pl

    @dataclass
    class Item:
        key: int
        value: str

    df = build_polars_dataframe(Item, size=5)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 5
    assert "key" in df.columns and "value" in df.columns


def test_create_dataframe_from_dicts(skip_if_no_polars):
    from dataclasses import dataclass

    import polars as pl

    @polars_factory
    @dataclass
    class User:
        id: int
        name: str

    dicts = User.build_dicts(size=3)
    df = User.create_dataframe_from_dicts(dicts)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3


def test_build_dataframe_requires_polars():
    """build_dataframe raises when Polars is not available."""
    from dataclasses import dataclass

    @polars_factory
    @dataclass
    class User:
        id: int
        name: str

    # If polars is installed, this passes; otherwise we'd need to mock
    try:
        import polars  # noqa: F401
        pytest.skip("Polars is installed, cannot test missing case")
    except ImportError:
        with pytest.raises(PolarsNotAvailableError):
            User.build_dataframe(size=5)


def test_classic_factory(skip_if_no_polars):
    from dataclasses import dataclass

    import polars as pl

    @dataclass
    class Product:
        product_id: int
        name: str
        price: float

    class ProductFactory(PolarsFactory[Product]):
        __model__ = Product

    df = ProductFactory.build_dataframe(size=20)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 20
    assert df.columns == ["product_id", "name", "price"]


def test_build_lazy_dataframe(skip_if_no_polars):
    from dataclasses import dataclass

    import polars as pl

    @polars_factory
    @dataclass
    class User:
        id: int
        name: str

    lf = User.build_lazy_dataframe(size=7)
    assert isinstance(lf, pl.LazyFrame)
    df = lf.collect()
    assert df.height == 7
    assert df.columns == ["id", "name"]


def test_build_dataframe_chunk_size(skip_if_no_polars):
    from dataclasses import dataclass

    import polars as pl

    @polars_factory
    @dataclass
    class Row:
        id: int
        value: float

    df = Row.build_dataframe(size=100, chunk_size=30)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 100
    assert df.columns == ["id", "value"]
    assert df["id"].dtype == pl.Int64
    assert df["value"].dtype == pl.Float64


def test_build_dataframe_schema_overrides(skip_if_no_polars):
    from dataclasses import dataclass

    import polars as pl

    @polars_factory
    @dataclass
    class User:
        id: int
        name: str

    df = User.build_dataframe(size=5, schema_overrides={"name": pl.Categorical})
    assert df.height == 5
    assert df["name"].dtype == pl.Categorical
