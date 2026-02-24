# Polypolars roadmap

High-level plan for polypolars. Priorities may shift based on feedback and usage.

Many items below have been implemented (see CHANGELOG and the repo).

---

## Current (v0.1.0)

- **Factory API**: `@polars_factory` decorator, `PolarsFactory` class, `build_polars_dataframe()`
- **Schema inference**: Dataclasses, Pydantic v2, TypedDict → Polars dtypes (including `Optional`, `List`, `Dict`, nested structs)
- **Data generation**: `build_dataframe(size=...)`, `build_dicts()`, `create_dataframe_from_dicts()`
- **I/O**: Parquet, JSON, CSV (save/load); `load_and_validate()`; JSON lines for dicts
- **Testing**: `assert_dataframe_equal`, `assert_schema_equal`, `assert_approx_count`, `assert_column_exists`, `assert_no_duplicates`, `get_column_stats`

---

## Near term

- **Stability**: Broader test coverage (edge cases, nested types, Pydantic), CI (e.g. GitHub Actions)
- **Docs**: API reference (e.g. Sphinx or MkDocs), more examples (nested structs, custom providers)
- **Polish**: Clearer errors for unsupported types; CHANGELOG and semantic versioning

---

## Later / ideas

- **CLI**: Schema export, validate file vs model, generate sample data from a model (similar to polyspark CLI)
- **Polars LazyFrame**: Helpers or factory path for `LazyFrame` where useful
- **Type mapping**: More Polars dtypes (e.g. `Categorical`, `Array` with fixed shape) and configuration
- **Performance**: Optional batching or chunking for very large `size`; benchmarks
- **Community**: Contributing guide, code of conduct, issue/PR templates

---

*Last updated: 2025*
