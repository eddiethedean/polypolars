"""Custom exceptions for polypolars."""


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
    """Raised when a type is not supported for conversion."""

    pass
