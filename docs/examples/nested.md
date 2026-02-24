# Example: Nested structs

```python
from dataclasses import dataclass
import polars as pl
from polypolars import polars_factory

@dataclass
class Address:
    street: str
    city: str
    zipcode: str

@polars_factory
@dataclass
class Person:
    id: int
    name: str
    address: Address

# Schema infers address as Struct(street, city, zipcode)
df = Person.build_dataframe(size=100)

# Query nested struct fields (use pl.col(...).struct.field(...))
df.select("name", pl.col("address").struct.field("city"), pl.col("address").struct.field("street"))
```

Example output (data varies per run):

```
shape: (3, 3)
┌──────────────────────┬──────────────────────┬──────────────────────┐
│ name                 ┆ city                 ┆ street               │
│ ---                  ┆ ---                  ┆ ---                  │
│ str                  ┆ str                  ┆ str                  │
╞══════════════════════╪══════════════════════╪══════════════════════╡
│ TyNogmkcpNApbMVIrdoK ┆ oUlYrLzOIdBwEhwaHDIp ┆ XCwUtmbzWVomVoMqsyJW │
│ UAgndTNMnObGcIGnTDLr ┆ GnuxqoGTHTBgttFwMTNX ┆ tgHfzpPxOJokfOQqlVqW │
│ cvhhKUOIRePqlabAmJuD ┆ DWPRzQhhTPbpSWCAGNCH ┆ pIwWGSSJWIXOgSWnaVnu │
└──────────────────────┴──────────────────────┴──────────────────────┘
```

## Array of structs

```python
from typing import List

@dataclass
class Item:
    name: str
    quantity: int

@polars_factory
@dataclass
class Order:
    order_id: int
    items: List[Item]

df = Order.build_dataframe(size=50)
# items column is List(Struct(...))
```

Example output (data varies per run):

```
shape: (3, 2)
┌──────────┬─────────────────────────────────┐
│ order_id ┆ items                           │
│ ---      ┆ ---                             │
│ i64      ┆ list[struct[2]]                 │
╞══════════╪═════════════════════════════════╡
│ 2915     ┆ [{"fCybdrchwdOCIfyGWpQD",3997}… │
│ 6        ┆ [{"ugJxvdhNjZkzIwrGpYfq",3631}… │
│ 473      ┆ [{"NWLALDnKraMBLlTJpirG",8657}… │
└──────────┴─────────────────────────────────┘
```
