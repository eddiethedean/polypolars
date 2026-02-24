"""Shared pytest fixtures for polypolars tests."""

import pytest


@pytest.fixture
def skip_if_no_polars():
    """Skip the test if Polars is not installed."""
    pytest.importorskip("polars")
