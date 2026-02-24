# Quick Start

## Decorator (recommended)

```python
from dataclasses import dataclass
from typing import Optional
from polypolars import polars_factory

@polars_factory
@dataclass
class Product:
    product_id: int
    name: str
    price: float
    description: Optional[str] = None
    in_stock: bool = True

# Build Polars DataFrame
df = Product.build_dataframe(size=100)

# Or LazyFrame
lf = Product.build_lazy_dataframe(size=100)

# Or dicts
dicts = Product.build_dicts(size=50)
```

Example output (first 5 rows; data varies per run):

```
shape: (5, 5)
┌────────────┬──────────────────────┬──────────────┬──────────────────────┬──────────┐
│ product_id ┆ name                 ┆ price        ┆ description          ┆ in_stock │
│ ---        ┆ ---                  ┆ ---          ┆ ---                  ┆ ---      │
│ i64        ┆ str                  ┆ f64          ┆ str                  ┆ bool     │
╞════════════╪══════════════════════╪══════════════╪══════════════════════╪══════════╡
│ 5582       ┆ hKJsoOOXlwgLIiiWOCJP ┆ 2.2760e8     ┆ rTUACBLlGBlHXIjzVvPt ┆ false    │
│ 7099       ┆ ZgUiDVJirxAYRrWIPnpS ┆ 274887.17671 ┆ bHGMXNFRLSDifpywMZrY ┆ true     │
...
└────────────┴──────────────────────┴──────────────┴──────────────────────┴──────────┘
```

## Classic factory class

```python
from polypolars import PolarsFactory

class ProductFactory(PolarsFactory[Product]):
    __model__ = Product

df = ProductFactory.build_dataframe(size=100)
```

## Convenience function

```python
from polypolars import build_polars_dataframe

df = build_polars_dataframe(Product, size=100)
```

## Schema overrides

Use `schema_overrides` to force a column to a specific Polars type (e.g. `Categorical`):

```python
import polars as pl

df = Product.build_dataframe(
    size=100,
    schema_overrides={"name": pl.Categorical}
)
```

## Chunked building

For very large sizes, use `chunk_size` to build in chunks and reduce memory:

```python
df = Product.build_dataframe(size=1_000_000, chunk_size=10_000)
```
