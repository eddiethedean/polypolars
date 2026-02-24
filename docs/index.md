# Polypolars

**Generate type-safe Polars DataFrames using polyfactory.**

Polypolars lets you create realistic test DataFrames from your Python data models—with **automatic schema inference** for Polars. Inspired by [polyspark](https://github.com/eddiethedean/polyspark).

## Features

- **Factory pattern**: Use [polyfactory](https://github.com/litestar-org/polyfactory) for data generation
- **Type-safe schema**: Python types become Polars dtypes automatically
- **Nullable handling**: `Optional[T]` and defaults are reflected in the schema
- **Complex types**: Nested structs, lists, dicts (as list-of-structs), `Tuple` → fixed-size `Array`
- **Multiple models**: Dataclasses, Pydantic v2, TypedDict
- **LazyFrame**: `build_lazy_dataframe()` for lazy evaluation
- **Chunked building**: Optional `chunk_size` for very large row counts
- **CLI**: Schema export, validate file vs model, generate sample data

## Installation

```bash
pip install polypolars
```

## Quick example

```python
from dataclasses import dataclass
from polypolars import polars_factory

@polars_factory
@dataclass
class User:
    id: int
    name: str
    email: str

df = User.build_dataframe(size=1000)
```

Example output (first 5 rows; data varies per run):

```
shape: (5, 3)
┌──────┬──────────────────────┬──────────────────────┐
│ id   ┆ name                 ┆ email                │
│ ---  ┆ ---                  ┆ ---                  │
│ i64  ┆ str                  ┆ str                  │
╞══════╪══════════════════════╪══════════════════════╡
│ 3167 ┆ QmYHeLMDMxWChjihAFxU ┆ vHGMKHjXsMBlxLuhqpUE │
│ 1028 ┆ hvLXPtlqURtwzqeyJruo ┆ ePDAdtelIEiRfEuAgoPz │
...
└──────┴──────────────────────┴──────────────────────┘
```

See [Quick Start](quickstart.md) and [API Reference](api/factory.md) for more.
