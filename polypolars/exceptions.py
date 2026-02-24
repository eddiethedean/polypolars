"""Custom exceptions for polypolars."""

from typing import Optional


class PolypolarsError(Exception):
    """Base exception for polypolars."""

    pass


class PolarsNotAvailableError(PolypolarsError):
    """Raised when Polars is required but not installed."""

    def __init__(
        self,
        message: str = "Polars is not installed. Install it with: pip install polars",
    ):
        super().__init__(message)


class SchemaInferenceError(PolypolarsError):
    """Raised when schema cannot be inferred from a type."""

    pass


class UnsupportedTypeError(PolypolarsError):
    """Raised when a type is not supported for conversion to a Polars dtype.

    The error message includes the unsupported type and a hint listing
    supported types (str, int, float, bool, list, dict, Optional, dataclass, etc.).
    """

    SUPPORTED_HINT = (
        "Supported types: str, int, float, bool, bytes, date, datetime, Decimal, "
        "List[T], Dict[K,V], Optional[T], Literal[...], dataclasses, Pydantic models, TypedDict."
    )

    def __init__(self, message: str, type_hint: Optional[type] = None):
        if type_hint is not None:
            name = getattr(type_hint, "__name__", None) or repr(type_hint)
            message = f"{message} Type: {name}. "
        super().__init__(f"{message}{self.SUPPORTED_HINT}")
