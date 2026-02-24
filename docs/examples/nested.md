# Example: Nested structs

```python
from dataclasses import dataclass
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

# Query nested fields
df.select("name", "address.city", "address.street")
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
