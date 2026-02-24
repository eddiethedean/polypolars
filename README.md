# Polypolars

**Generate type-safe Polars DataFrames effortlessly using polyfactory**

Inspired by [polyspark](https://github.com/eddiethedean/polyspark), polypolars lets you create realistic test DataFrames from your Python data models—with **automatic schema inference** for Polars.

```python
from dataclasses import dataclass
from polypolars import polars_factory

@polars_factory
@dataclass
class User:
    id: int
    name: str
    email: str

# Generate 1000 rows instantly:
df = User.build_dataframe(size=1000)
print(df.head())
```

## Why Polypolars?

- **Factory pattern**: Leverage [polyfactory](https://github.com/litestar-org/polyfactory) for data generation
- **Type-safe schema**: Python types become Polars dtypes automatically
- **Nullable handling**: `Optional[T]` and defaults are reflected in the schema
- **Complex types**: Nested structs, lists, and dicts (as list-of-structs)
- **Multiple models**: Dataclasses, Pydantic, and TypedDict

## Installation

```bash
pip install polypolars
```

For development:

```bash
pip install "polypolars[dev]"
```

## Quick Start

### Decorator (recommended)

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
print(df.head())

# Or get dicts
dicts = Product.build_dicts(size=50)
```

### Classic factory class

```python
from polypolars import PolarsFactory

class ProductFactory(PolarsFactory[Product]):
    __model__ = Product

df = ProductFactory.build_dataframe(size=100)
```

### Convenience function

```python
from polypolars import build_polars_dataframe

df = build_polars_dataframe(Product, size=100)
```

## Schema inference

Schema is inferred from your type hints, so all-null columns still get the correct type:

```python
@polars_factory
@dataclass
class User:
    id: int
    email: Optional[str]  # nullable string in Polars

df = User.build_dataframe(size=100)  # schema: id Int64, email String
```

## From dicts

```python
dicts = Product.build_dicts(size=1000)
# Convert to DataFrame when needed:
df = Product.create_dataframe_from_dicts(dicts)
```

## Pydantic

```python
from pydantic import BaseModel, Field
from polypolars import polars_factory

@polars_factory
class User(BaseModel):
    id: int = Field(gt=0)
    username: str = Field(min_length=3, max_length=20)
    email: str
    is_active: bool = True

df = User.build_dataframe(size=500)
```

## Type mapping

| Python       | Polars     |
|-------------|------------|
| `str`       | `String`   |
| `int`       | `Int64`    |
| `float`     | `Float64`  |
| `bool`      | `Boolean`  |
| `datetime`  | `Datetime` |
| `date`      | `Date`     |
| `List[T]`   | `List(T)`  |
| `Dict[K,V]` | `List(Struct(key, value))` |
| `Optional[T]` | `T` (nullable) |
| Dataclass / Pydantic | `Struct(...)` |

## I/O and testing

```python
from polypolars import (
    save_as_parquet,
    load_parquet,
    load_and_validate,
    infer_schema,
    assert_dataframe_equal,
    assert_schema_equal,
)

df = User.build_dataframe(size=1000)
save_as_parquet(df, "users.parquet")

# Load and validate
schema = infer_schema(User)
df2 = load_and_validate("users.parquet", expected_schema=schema)

assert_dataframe_equal(df, df2, check_order=False)
```

## License

MIT

## Related

- [polyspark](https://github.com/eddiethedean/polyspark) – inspiration for this library
- [polyfactory](https://github.com/litestar-org/polyfactory) – factory library for mock data
- [Polars](https://www.pola.rs/) – fast DataFrame library
