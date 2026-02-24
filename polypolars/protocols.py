"""Protocol definitions for Polars types to avoid hard dependency."""

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


def is_polars_available() -> bool:
    """Check if Polars is available at runtime.

    Returns:
        bool: True if polars can be imported, False otherwise.
    """
    try:
        import polars  # noqa: F401

        return True
    except ImportError:
        return False


def get_polars_module() -> Optional[Any]:
    """Get the polars module if available.

    Returns:
        The polars module if available, None otherwise.
    """
    if is_polars_available():
        import polars as pl

        return pl
    return None


@runtime_checkable
class DataFrameProtocol(Protocol):
    """Protocol matching Polars DataFrame interface."""

    columns: List[str]
    schema: Dict[str, Any]

    @property
    def height(self) -> int:
        """Return the number of rows."""
        ...

    def width(self) -> int:
        """Return the number of columns."""
        ...

    def to_dicts(self) -> List[Dict[str, Any]]:
        """Convert to list of row dicts."""
        ...

    def write_parquet(self, path: str, **kwargs: Any) -> None:
        """Write to Parquet."""
        ...

    def write_json(self, path: str, **kwargs: Any) -> None:
        """Write to JSON."""
        ...

    def write_csv(self, path: str, **kwargs: Any) -> None:
        """Write to CSV."""
        ...

    def filter(self, *predicates: Any) -> "DataFrameProtocol":
        """Filter rows."""
        ...

    def select(self, *exprs: Any) -> "DataFrameProtocol":
        """Select columns."""
        ...

    def sort(self, *columns: str, **kwargs: Any) -> "DataFrameProtocol":
        """Sort rows."""
        ...

    def unique(self, subset: Optional[List[str]] = None, **kwargs: Any) -> "DataFrameProtocol":
        """Return unique rows."""
        ...
