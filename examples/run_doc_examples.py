"""Run the code from the docs to ensure all examples execute without errors."""

from dataclasses import dataclass
from typing import List, Optional

import polars as pl

from polypolars import PolarsFactory, build_polars_dataframe, polars_factory


def main():
    # Quick Start: decorator
    @polars_factory
    @dataclass
    class Product:
        product_id: int
        name: str
        price: float
        description: Optional[str] = None
        in_stock: bool = True

    df = Product.build_dataframe(size=10)
    assert df.shape[0] == 10
    lf = Product.build_lazy_dataframe(size=10)
    assert lf.collect().shape[0] == 10
    dicts = Product.build_dicts(size=5)
    assert len(dicts) == 5

    # Classic factory class
    class ProductFactory(PolarsFactory[Product]):
        __model__ = Product

    df2 = ProductFactory.build_dataframe(size=10)
    assert df2.shape[0] == 10

    # Convenience function
    df3 = build_polars_dataframe(Product, size=10)
    assert df3.shape[0] == 10

    # Schema overrides
    df4 = Product.build_dataframe(size=5, schema_overrides={"name": pl.Categorical})
    assert df4.schema["name"] == pl.Categorical

    # Nested structs (docs/examples/nested.md)
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

    df_person = Person.build_dataframe(size=5)
    out = df_person.select(
        "name",
        pl.col("address").struct.field("city"),
        pl.col("address").struct.field("street"),
    )
    assert out.shape[0] == 5

    # Array of structs
    @dataclass
    class Item:
        name: str
        quantity: int

    @polars_factory
    @dataclass
    class Order:
        order_id: int
        items: List[Item]

    df_order = Order.build_dataframe(size=5)
    assert df_order.shape[0] == 5 and "items" in df_order.columns

    # Custom providers (simplified)
    @polars_factory
    @dataclass
    class User:
        id: int
        name: str
        email: str

    df_user = User.build_dataframe(size=20)
    assert df_user.shape[0] == 20

    print("All doc examples ran successfully.")


if __name__ == "__main__":
    main()
