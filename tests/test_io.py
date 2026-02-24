"""Tests for I/O utilities."""

import tempfile
from pathlib import Path

import pytest

pytest.importorskip("polars")

from dataclasses import dataclass

from polypolars import (
    DataIOError,
    infer_schema,
    load_and_validate,
    load_csv,
    load_dicts_from_json,
    load_json,
    load_parquet,
    polars_factory,
    save_as_csv,
    save_as_json,
    save_as_parquet,
    save_dicts_as_json,
)


@pytest.fixture
def sample_df():
    @polars_factory
    @dataclass
    class Row:
        id: int
        name: str
        value: float

    return Row.build_dataframe(size=10)


@pytest.mark.parametrize("fmt,ext,save_fn,load_fn", [
    ("parquet", "parquet", save_as_parquet, load_parquet),
    ("json", "json", save_as_json, load_json),
    ("csv", "csv", save_as_csv, load_csv),
])
def test_save_load_formats(sample_df, fmt, ext, save_fn, load_fn):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / f"out.{ext}"
        save_fn(sample_df, str(path))
        assert path.exists()
        df = load_fn(str(path))
        assert df.height == sample_df.height
        assert set(df.columns) == set(sample_df.columns)


def test_save_load_parquet(sample_df):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "out.parquet"
        save_as_parquet(sample_df, str(path))
        assert path.exists()
        df = load_parquet(str(path))
        assert df.height == sample_df.height
        assert df.columns == sample_df.columns


def test_save_load_json(sample_df):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "out.json"
        save_as_json(sample_df, str(path))
        assert path.exists()
        df = load_json(str(path))
        assert df.height == sample_df.height


def test_save_load_csv(sample_df):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "out.csv"
        save_as_csv(sample_df, str(path))
        assert path.exists()
        df = load_csv(str(path))
        assert df.height == sample_df.height


def test_load_and_validate_parquet(sample_df):
    @dataclass
    class Row:
        id: int
        name: str
        value: float

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "out.parquet"
        save_as_parquet(sample_df, str(path))
        schema = infer_schema(Row)
        df = load_and_validate(str(path), expected_schema=schema)
        assert df.height == sample_df.height


def test_save_load_dicts_json():
    data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "out.jsonl"
        save_dicts_as_json(data, str(path))
        assert path.exists()
        loaded = load_dicts_from_json(str(path))
        assert loaded == data


def test_load_unsupported_format():
    with pytest.raises(DataIOError):
        load_and_validate("/nonexistent.xyz")


def test_load_and_validate_schema_mismatch(sample_df):
    """load_and_validate raises when file schema does not match expected_schema."""
    @dataclass
    class OtherModel:
        id: int
        name: str
        value: str  # different type than Row.value (float)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "out.parquet"
        save_as_parquet(sample_df, str(path))
        expected_schema = infer_schema(OtherModel)
        with pytest.raises(DataIOError) as exc_info:
            load_and_validate(str(path), expected_schema=expected_schema, validate_schema=True)
        assert "validation" in str(exc_info.value).lower() or "schema" in str(exc_info.value).lower()


def test_load_dicts_from_json_missing_file():
    with pytest.raises(DataIOError) as exc_info:
        load_dicts_from_json("/nonexistent_file_xyz.jsonl")
    assert "not found" in str(exc_info.value).lower() or "exist" in str(exc_info.value).lower()
