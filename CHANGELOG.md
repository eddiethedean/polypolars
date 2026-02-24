# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- CLI: `polypolars schema export`, `schema validate`, `generate` (see README).
- LazyFrame support: `build_lazy_dataframe()` and decorator method.
- Type mapping: `Categorical`, `Array` (fixed shape) and optional schema overrides.
- Performance: chunked building for large `size` via `build_dataframe(..., chunk_size=...)`.
- Broader test coverage: nested types, Pydantic, I/O, testing helpers, edge cases.
- GitHub Actions CI (test matrix, ruff).
- MkDocs API docs and examples (nested structs, custom providers).
- CONTRIBUTING.md, CODE_OF_CONDUCT.md, issue/PR templates.

### Changed

- Clearer `UnsupportedTypeError` messages with supported-type hint.

## [0.1.0] - 2025-02-24

### Added

- Initial release.
- Factory API: `@polars_factory`, `PolarsFactory`, `build_polars_dataframe()`.
- Schema inference from dataclasses, Pydantic v2, TypedDict.
- `build_dataframe()`, `build_dicts()`, `create_dataframe_from_dicts()`.
- I/O: Parquet, JSON, CSV; `load_and_validate()`; dict JSON lines.
- Testing: `assert_dataframe_equal`, `assert_schema_equal`, `assert_approx_count`, etc.
