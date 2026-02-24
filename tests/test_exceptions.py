"""Tests for exception types and messages."""

import pytest

from polypolars.exceptions import UnsupportedTypeError


def test_unsupported_type_error_message_contains_hint(skip_if_no_polars):
    """UnsupportedTypeError message includes supported-type hint."""
    class CustomType:
        pass

    from dataclasses import dataclass

    from polypolars import infer_schema

    @dataclass
    class M:
        x: CustomType

    with pytest.raises(UnsupportedTypeError) as exc_info:
        infer_schema(M)

    msg = str(exc_info.value)
    assert "Supported types" in msg or "str, int, float" in msg
    assert "CustomType" in msg or "Custom" in msg
