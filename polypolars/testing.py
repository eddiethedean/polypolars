"""Testing utilities for Polars DataFrames."""

from typing import Any, Dict, List, Optional

from polypolars.exceptions import PolypolarsError
from polypolars.protocols import DataFrameProtocol, is_polars_available


class DataFrameComparisonError(PolypolarsError):
    """Raised when DataFrame comparison fails."""

    pass


def _floats_are_close(a: float, b: float, rtol: float, atol: float) -> bool:
    return abs(a - b) <= (atol + rtol * abs(b))


def assert_dataframe_equal(
    df1: DataFrameProtocol,
    df2: DataFrameProtocol,
    check_order: bool = False,
    rtol: float = 1e-5,
    atol: float = 1e-8,
    check_column_order: bool = False,
) -> None:
    """Assert that two Polars DataFrames are equal."""
    if not is_polars_available():
        raise DataFrameComparisonError(
            "Polars is required for DataFrame comparison. Install it with: pip install polars"
        )

    count1 = df1.height
    count2 = df2.height

    if count1 != count2:
        raise DataFrameComparisonError(f"DataFrame row counts don't match: {count1} != {count2}")

    try:
        assert_schema_equal(df1.schema, df2.schema, check_order=check_column_order)
    except DataFrameComparisonError as e:
        raise DataFrameComparisonError(f"DataFrame schemas don't match:\n{str(e)}") from e

    if not check_column_order:
        cols = sorted(df1.columns)
        df1 = df1.select(cols)
        df2 = df2.select(cols)

    if not check_order and count1 > 0:
        by = df1.columns
        df1 = df1.sort(by)  # type: ignore[arg-type]
        df2 = df2.sort(by)  # type: ignore[arg-type]

    rows1 = df1.to_dicts()
    rows2 = df2.to_dicts()

    differences = []
    for i, (row1, row2) in enumerate(zip(rows1, rows2)):
        for col_name in row1.keys():
            val1 = row1[col_name]
            val2 = row2[col_name]
            if val1 != val2:
                if isinstance(val1, float) and isinstance(val2, float):
                    if not _floats_are_close(val1, val2, rtol, atol):
                        differences.append(f"  Row {i}, column '{col_name}': {val1} != {val2}")
                else:
                    differences.append(f"  Row {i}, column '{col_name}': {val1} != {val2}")

    if differences:
        error_msg = "DataFrames have differing values:\n" + "\n".join(differences[:10])
        if len(differences) > 10:
            error_msg += f"\n ... and {len(differences) - 10} more differences"
        raise DataFrameComparisonError(error_msg)


def assert_schema_equal(
    schema1: Any,
    schema2: Any,
    check_order: bool = False,
) -> None:
    """Assert that two Polars schemas (dict or schema attribute) are equal."""
    if not is_polars_available():
        raise DataFrameComparisonError(
            "Polars is required for schema comparison. Install it with: pip install polars"
        )

    # Polars schema is dict-like: name -> dtype
    items1 = list(schema1.items()) if hasattr(schema1, "items") else []
    items2 = list(schema2.items()) if hasattr(schema2, "items") else []

    if len(items1) != len(items2):
        raise DataFrameComparisonError(
            f"Schemas have different number of fields: {len(items1)} != {len(items2)}"
        )

    names1 = {k for k, _ in items1}
    names2 = {k for k, _ in items2}
    if names1 != names2:
        raise DataFrameComparisonError(f"Schemas have different field names: {names1 ^ names2}")

    schema1_map = dict(items1)
    schema2_map = dict(items2)

    for name in names1:
        t1 = schema1_map[name]
        t2 = schema2_map[name]
        if t1 != t2:
            raise DataFrameComparisonError(f"Field '{name}' has different types: {t1} vs {t2}")

    if check_order and items1 != items2:
        raise DataFrameComparisonError("Schema field order differs")


def assert_approx_count(df: DataFrameProtocol, expected_count: int, tolerance: float = 0.1) -> None:
    """Assert that DataFrame row count is approximately equal to expected."""
    actual_count = df.height
    min_count = int(expected_count * (1 - tolerance))
    max_count = int(expected_count * (1 + tolerance))

    if not (min_count <= actual_count <= max_count):
        raise DataFrameComparisonError(
            f"DataFrame count {actual_count} is not within {tolerance * 100:.1f}% "
            f"of expected {expected_count}"
        )


def get_column_stats(df: DataFrameProtocol, column: str) -> Dict[str, Any]:
    """Get basic statistics for a column."""
    if not is_polars_available():
        raise DataFrameComparisonError("Polars is required for column stats")

    import polars as pl

    total_count = df.height
    s = df[column]  # type: ignore[index]
    null_count = s.null_count()
    non_null_count = total_count - null_count
    distinct_count = s.n_unique()

    stats: Dict[str, Any] = {
        "count": total_count,
        "non_null_count": non_null_count,
        "null_count": null_count,
        "distinct_count": distinct_count,
    }

    if s.dtype in (pl.Int64, pl.Int32, pl.Float64, pl.Float32):
        stats["min"] = s.min()
        stats["max"] = s.max()
        stats["mean"] = s.mean()
        stats["std"] = s.std()

    return stats


def assert_column_exists(df: DataFrameProtocol, *columns: str) -> None:
    """Assert that specified columns exist in DataFrame."""
    df_columns = set(df.columns)
    missing = [c for c in columns if c not in df_columns]
    if missing:
        raise DataFrameComparisonError(
            f"Columns missing from DataFrame: {missing}. Available: {sorted(df_columns)}"
        )


def assert_no_duplicates(
    df: DataFrameProtocol,
    columns: Optional[List[str]] = None,
) -> None:
    """Assert that DataFrame has no duplicate rows."""
    if columns is None:
        unique_count = df.unique().height
    else:
        unique_count = df.unique(subset=columns).height
    total_count = df.height

    if unique_count != total_count:
        raise DataFrameComparisonError(
            f"DataFrame contains {total_count - unique_count} duplicate row(s)"
        )
