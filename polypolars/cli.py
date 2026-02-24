"""Command-line interface for polypolars."""

import importlib
import sys
from pathlib import Path
from typing import Optional


def _resolve_model(module_dot_model: str):
    """Load model class from 'module.submodule:ClassName'."""
    if ":" not in module_dot_model:
        print("Error: expected module:ModelName (e.g. myapp.models:User)", file=sys.stderr)
        sys.exit(1)
    mod_path, cls_name = module_dot_model.rsplit(":", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        print(f"Error: could not import module {mod_path}: {e}", file=sys.stderr)
        sys.exit(1)
    if not hasattr(mod, cls_name):
        print(f"Error: module {mod_path} has no attribute {cls_name}", file=sys.stderr)
        sys.exit(1)
    return getattr(mod, cls_name)


def _schema_export(module_dot_model: str, output: Optional[str]) -> int:
    """Export schema as Polars schema dict repr or DDL-like string to file."""
    from polypolars import infer_schema

    model = _resolve_model(module_dot_model)
    schema = infer_schema(model)
    lines = [f"{name}: {dtype}" for name, dtype in schema.items()]
    text = "\n".join(lines)
    if output:
        Path(output).write_text(text)
        print(f"Schema written to {output}")
    else:
        print(text)
    return 0


def _schema_validate(module_dot_model: str, path: str) -> int:
    """Validate a data file against the model schema."""
    from polypolars import infer_schema, load_and_validate

    model = _resolve_model(module_dot_model)
    expected_schema = infer_schema(model)
    try:
        load_and_validate(path, expected_schema=expected_schema, validate_schema=True)
        print("Validation passed.")
        return 0
    except Exception as e:
        print(f"Validation failed: {e}", file=sys.stderr)
        return 1


def _generate(
    module_dot_model: str,
    size: int,
    output: str,
    format: str,
) -> int:
    """Generate sample data and write to file."""
    import polars as pl

    model = _resolve_model(module_dot_model)
    from polypolars import build_polars_dataframe

    df = build_polars_dataframe(model, size=size)
    format = format.lower()
    if format == "parquet":
        df.write_parquet(output)
    elif format == "json":
        df.write_json(output)
    elif format == "csv":
        df.write_csv(output)
    else:
        print(f"Error: unsupported format {format}. Use parquet, json, or csv.", file=sys.stderr)
        return 1
    print(f"Generated {size} rows to {output}")
    return 0


def main() -> int:
    """Entry point for the polypolars CLI."""
    import argparse

    parser = argparse.ArgumentParser(prog="polypolars", description="Polypolars CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    # schema export
    schema = sub.add_parser("schema", help="Schema operations")
    schema_sub = schema.add_subparsers(dest="schema_cmd", required=True)
    export_p = schema_sub.add_parser("export", help="Export model schema")
    export_p.add_argument("model", help="Module:ModelName (e.g. myapp.models:User)")
    export_p.add_argument("--output", "-o", help="Write to file")
    export_p.set_defaults(func=lambda a: _schema_export(a.model, a.output))

    # schema validate
    validate_p = schema_sub.add_parser("validate", help="Validate file against model schema")
    validate_p.add_argument("model", help="Module:ModelName")
    validate_p.add_argument("path", help="Path to parquet/json/csv file")
    validate_p.set_defaults(func=lambda a: _schema_validate(a.model, a.path))

    # generate
    gen_p = sub.add_parser("generate", help="Generate sample data from model")
    gen_p.add_argument("model", help="Module:ModelName")
    gen_p.add_argument("--size", "-n", type=int, default=100, help="Number of rows")
    gen_p.add_argument("--output", "-o", required=True, help="Output file path")
    gen_p.add_argument("--format", "-f", default="parquet", choices=["parquet", "json", "csv"])
    gen_p.set_defaults(func=lambda a: _generate(a.model, a.size, a.output, a.format))

    args = parser.parse_args()
    if args.command == "schema":
        return args.func(args)
    if args.command == "generate":
        return args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
