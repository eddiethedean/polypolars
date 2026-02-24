"""Basic usage of polypolars with dataclasses."""

from dataclasses import dataclass
from typing import Optional

from polypolars import polars_factory

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


@polars_factory
@dataclass
class User:
    id: int
    name: str
    email: str
    age: Optional[int] = None
    active: bool = True


def main():
    # Generate dicts
    dicts = User.build_dicts(size=5)
    print("Generated dicts:", dicts[:2])

    if HAS_POLARS:
        # Build Polars DataFrame
        df = User.build_dataframe(size=100)
        print("\nDataFrame shape:", df.shape)
        print(df.head())


if __name__ == "__main__":
    main()
