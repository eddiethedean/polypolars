"""Data export and import utilities for polypolars."""

from pathlib import Path
from typing import Any, Dict, List, Optional

from polypolars.exceptions import PolypolarsError
from polypolars.protocols import DataFrameProtocol, is_polars_available


class DataIOError(PolypolarsError):
    """Raised when data I/O operations fail."""

    pass


def save_as_parquet(
    df: DataFrameProtocol,
    path: str,
    **options: Any,
) -> None:
    """Save DataFrame as Parquet file."""
    if not is_polars_available():
        raise DataIOError("Polars is required for Parquet operations")

    try:
        df.write_parquet(path, **options)
    except Exception as e:
        raise DataIOError(f"Failed to save Parquet file: {e}") from e


def save_as_json(df: DataFrameProtocol, path: str, **options: Any) -> None:
    """Save DataFrame as JSON file."""
    if not is_polars_available():
        raise DataIOError("Polars is required for JSON operations")

    try:
        df.write_json(path, **options)
    except Exception as e:
        raise DataIOError(f"Failed to save JSON file: {e}") from e


def save_as_csv(df: DataFrameProtocol, path: str, header: bool = True, **options: Any) -> None:
    """Save DataFrame as CSV file."""
    if not is_polars_available():
        raise DataIOError("Polars is required for CSV operations")

    try:
        df.write_csv(path, include_header=header, **options)
    except Exception as e:
        raise DataIOError(f"Failed to save CSV file: {e}") from e


def load_parquet(path: str, **options: Any) -> Any:
    """Load DataFrame from Parquet file."""
    if not is_polars_available():
        raise DataIOError("Polars is required for Parquet operations")

    try:
        import polars as pl

        return pl.read_parquet(path, **options)
    except Exception as e:
        raise DataIOError(f"Failed to load Parquet file: {e}") from e


def load_json(path: str, **options: Any) -> Any:
    """Load DataFrame from JSON file."""
    if not is_polars_available():
        raise DataIOError("Polars is required for JSON operations")

    try:
        import polars as pl

        return pl.read_json(path, **options)
    except Exception as e:
        raise DataIOError(f"Failed to load JSON file: {e}") from e


def load_csv(
    path: str,
    has_header: bool = True,
    **options: Any,
) -> Any:
    """Load DataFrame from CSV file."""
    if not is_polars_available():
        raise DataIOError("Polars is required for CSV operations")

    try:
        import polars as pl

        return pl.read_csv(path, has_header=has_header, **options)
    except Exception as e:
        raise DataIOError(f"Failed to load CSV file: {e}") from e


def load_and_validate(
    path: str,
    expected_schema: Optional[Any] = None,
    validate_schema: bool = True,
) -> Any:
    """Load data file and optionally validate against expected schema."""
    if not is_polars_available():
        raise DataIOError("Polars is required for data loading")

    try:
        file_path = Path(path)
        suffix = file_path.suffix.lower()

        if suffix == ".parquet":
            df = load_parquet(path)
        elif suffix == ".json":
            df = load_json(path)
        elif suffix == ".csv":
            df = load_csv(path)
        else:
            raise DataIOError(
                f"Unsupported file format: {suffix}. Supported: .parquet, .json, .csv"
            )

        if validate_schema and expected_schema is not None:
            from polypolars.testing import assert_schema_equal

            try:
                assert_schema_equal(expected_schema, df.schema, check_order=False)
            except Exception as e:
                raise DataIOError(f"Schema validation failed: {e}") from e

        return df

    except DataIOError:
        raise
    except Exception as e:
        raise DataIOError(f"Failed to load and validate data: {e}") from e


def save_dicts_as_json(data: List[Dict[str, Any]], path: str) -> None:
    """Save list of dictionaries as JSON lines file."""
    import json

    try:
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            for record in data:
                json.dump(record, f, default=str)
                f.write("\n")
    except Exception as e:
        raise DataIOError(f"Failed to save JSON file: {e}") from e


def load_dicts_from_json(path: str) -> List[Dict[str, Any]]:
    """Load list of dictionaries from JSON lines file."""
    import json

    try:
        file_path = Path(path)
        if not file_path.exists():
            raise DataIOError(f"File not found: {path}")
        data = []
        with open(file_path) as f:
            for line in f:
                if line.strip():
                    data.append(json.loads(line))
        return data
    except DataIOError:
        raise
    except Exception as e:
        raise DataIOError(f"Failed to load JSON file: {e}") from e
