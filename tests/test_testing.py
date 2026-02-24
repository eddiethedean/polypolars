"""Tests for testing utilities."""

import pytest

pytest.importorskip("polars")

from dataclasses import dataclass

import polars as pl

from polypolars import polars_factory
from polypolars.testing import (
    DataFrameComparisonError,
    assert_approx_count,
    assert_column_exists,
    assert_dataframe_equal,
    assert_no_duplicates,
    assert_schema_equal,
    get_column_stats,
)


@pytest.fixture
def df():
    @polars_factory
    @dataclass
    class Row:
        id: int
        name: str
        value: float

    return Row.build_dataframe(size=10)


def test_assert_dataframe_equal(df):
    df2 = df.clone()
    assert_dataframe_equal(df, df2)


def test_assert_dataframe_equal_different_order(df):
    df2 = df.sort("id")
    df_sorted = df.sort("id")
    assert_dataframe_equal(df2, df_sorted, check_order=True)


def test_assert_schema_equal(df):
    assert_schema_equal(df.schema, df.schema)


def test_assert_approx_count(df):
    assert_approx_count(df, 10, tolerance=0)
    assert_approx_count(df, 8, tolerance=0.3)


def test_assert_column_exists(df):
    assert_column_exists(df, "id", "name", "value")


def test_assert_column_exists_missing(df):
    with pytest.raises(DataFrameComparisonError):
        assert_column_exists(df, "id", "nonexistent")


def test_assert_no_duplicates(df):
    assert_no_duplicates(df)


def test_get_column_stats(df):
    stats = get_column_stats(df, "value")
    assert stats["count"] == 10
    assert "mean" in stats
    assert "min" in stats
    assert "max" in stats


def test_assert_dataframe_equal_different_row_counts(df):
    df_small = df.head(5)
    with pytest.raises(DataFrameComparisonError) as exc_info:
        assert_dataframe_equal(df, df_small)
    assert "row counts" in str(exc_info.value).lower() or "don't match" in str(exc_info.value)


def test_assert_dataframe_equal_different_schema(df):
    df_extra = df.with_columns(pl.lit(0).alias("extra"))
    with pytest.raises(DataFrameComparisonError):
        assert_dataframe_equal(df, df_extra)


def test_assert_dataframe_equal_different_value(df):
    df2 = df.clone()
    df2 = df2.with_columns(
        pl.when(pl.col("id") == df2["id"][0]).then(pl.lit(999)).otherwise(pl.col("id")).alias("id")
    )
    with pytest.raises(DataFrameComparisonError) as exc_info:
        assert_dataframe_equal(df, df2)
    assert "differing" in str(exc_info.value).lower() or "!=" in str(exc_info.value)


def test_assert_schema_equal_different_field_count(df):
    schema_extra = dict(df.schema)
    schema_extra["extra"] = pl.String
    with pytest.raises(DataFrameComparisonError):
        assert_schema_equal(df.schema, schema_extra)


def test_assert_schema_equal_different_field_names(df):
    other = {"id": pl.Int64, "label": pl.String, "value": pl.Float64}
    with pytest.raises(DataFrameComparisonError):
        assert_schema_equal(df.schema, other)


def test_assert_schema_equal_different_dtypes(df):
    other = {"id": pl.Int64, "name": pl.Int64, "value": pl.Float64}
    with pytest.raises(DataFrameComparisonError):
        assert_schema_equal(df.schema, other)


def test_assert_approx_count_fails_outside_tolerance(df):
    with pytest.raises(DataFrameComparisonError):
        assert_approx_count(df, 100, tolerance=0.1)


def test_assert_no_duplicates_fails_with_duplicates(df):
    dup = pl.concat([df.head(1), df.head(1)])
    with pytest.raises(DataFrameComparisonError):
        assert_no_duplicates(dup)
