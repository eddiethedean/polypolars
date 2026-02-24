"""Minimal model module for CLI tests. Importable as tests.cli_models."""

from dataclasses import dataclass


@dataclass
class CLIModel:
    """Simple model for polypolars CLI tests."""

    id: int
    name: str
    value: float
