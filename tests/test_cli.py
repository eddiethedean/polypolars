"""Tests for the polypolars CLI."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

pytest.importorskip("polars")

# Project root so subprocess can import tests.cli_models
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run_cli(args):
    """Run polypolars CLI; return (returncode, stdout, stderr)."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(_PROJECT_ROOT) + (os.pathsep + existing) if existing else str(_PROJECT_ROOT)
    result = subprocess.run(
        [sys.executable, "-m", "polypolars.cli"] + args,
        capture_output=True,
        text=True,
        cwd=str(_PROJECT_ROOT),
        env=env,
    )
    return result.returncode, result.stdout, result.stderr


def test_cli_schema_export_stdout():
    code, out, err = _run_cli(["schema", "export", "tests.cli_models:CLIModel"])
    assert code == 0
    assert "id:" in out
    assert "name:" in out
    assert "value:" in out


def test_cli_schema_export_to_file():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "schema.txt"
        code, out, err = _run_cli(["schema", "export", "tests.cli_models:CLIModel", "--output", str(path)])
        assert code == 0
        assert path.exists()
        text = path.read_text()
        assert "id:" in text
        assert "name:" in text
        assert "Schema written" in out


def test_cli_schema_validate_success():
    from polypolars import build_polars_dataframe, save_as_parquet
    from tests.cli_models import CLIModel

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "data.parquet"
        df = build_polars_dataframe(CLIModel, size=5)
        save_as_parquet(df, str(path))
        code, out, err = _run_cli(["schema", "validate", "tests.cli_models:CLIModel", str(path)])
        assert code == 0
        assert "Validation passed" in out


def test_cli_generate_parquet():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "out.parquet"
        code, out, err = _run_cli([
            "generate", "tests.cli_models:CLIModel", "--size", "10", "--output", str(path), "--format", "parquet"
        ])
        assert code == 0
        assert path.exists()
        import polars as pl
        df = pl.read_parquet(str(path))
        assert df.height == 10
        assert "Generated" in out


def test_cli_error_no_colon():
    code, out, err = _run_cli(["schema", "export", "invalid"])
    assert code != 0
    assert "expected" in err.lower() or "module" in err.lower()


def test_cli_error_missing_module():
    code, out, err = _run_cli(["schema", "export", "nonexistent_module_xyz:User"])
    assert code != 0
    assert "import" in err.lower() or "Error" in err


def test_cli_error_missing_class():
    code, out, err = _run_cli(["schema", "export", "tests.cli_models:NonExistentClass"])
    assert code != 0
    assert "attribute" in err.lower() or "Error" in err
