"""Polypolars - Generate Polars DataFrames using polyfactory.

Generate type-safe Polars DataFrames for testing and development from
dataclasses, Pydantic models, and TypedDicts—with automatic schema inference.
"""

from polypolars.exceptions import (
    PolarsNotAvailableError,
    PolypolarsError,
    SchemaInferenceError,
    UnsupportedTypeError,
)
from polypolars.factory import (
    PolarsFactory,
    build_polars_dataframe,
    polars_factory,
)
from polypolars.io import (
    DataIOError,
    load_and_validate,
    load_csv,
    load_dicts_from_json,
    load_json,
    load_parquet,
    save_as_csv,
    save_as_json,
    save_as_parquet,
    save_dicts_as_json,
)
from polypolars.protocols import is_polars_available
from polypolars.schema import (
    dataclass_to_struct_type,
    infer_schema,
    infer_schema_as_struct,
    pydantic_to_struct_type,
    python_type_to_polars_type,
    typed_dict_to_struct_type,
)
from polypolars.testing import (
    DataFrameComparisonError,
    assert_approx_count,
    assert_column_exists,
    assert_dataframe_equal,
    assert_no_duplicates,
    assert_schema_equal,
    get_column_stats,
)

__version__ = "0.1.0"

__all__ = [
    "PolarsFactory",
    "build_polars_dataframe",
    "polars_factory",
    "infer_schema",
    "infer_schema_as_struct",
    "dataclass_to_struct_type",
    "pydantic_to_struct_type",
    "python_type_to_polars_type",
    "typed_dict_to_struct_type",
    "assert_dataframe_equal",
    "assert_schema_equal",
    "assert_approx_count",
    "assert_column_exists",
    "assert_no_duplicates",
    "get_column_stats",
    "DataFrameComparisonError",
    "save_as_parquet",
    "save_as_json",
    "save_as_csv",
    "load_parquet",
    "load_json",
    "load_csv",
    "load_and_validate",
    "save_dicts_as_json",
    "load_dicts_from_json",
    "DataIOError",
    "is_polars_available",
    "PolypolarsError",
    "PolarsNotAvailableError",
    "SchemaInferenceError",
    "UnsupportedTypeError",
]
