"""Basic usage of polypolars with dataclasses.

Example output when run (data varies per run):

    Generated dicts: [{'id': 4360, 'name': 'axomjOzCFbMAmjBeukiy', 'email': 'SRLxdIkDgkhjFDXmrZGf', 'age': None, 'active': True}, ...]

    DataFrame shape: (100, 5)
    shape: (5, 5)
    ┌──────┬──────────────────────┬──────────────────────┬──────┬────────┐
    │ id   ┆ name                 ┆ email                ┆ age  ┆ active │
    │ ---  ┆ ---                  ┆ ---                  ┆ ---  ┆ ---    │
    │ i64  ┆ str                  ┆ str                  ┆ i64  ┆ bool   │
    ╞══════╪══════════════════════╪══════════════════════╪══════╪════════╡
    │ 3730 ┆ efEMwxidkTmUReJBGnVU ┆ RfdxUeKYWFTphGoxvaDl ┆ 9195 ┆ false  │
    │ 4520 ┆ oXneNDXiNvrZMhGjiOfQ ┆ RmGRlGGOMdBkSDEDhphs ┆ 5887 ┆ true   │
    ...
    └──────┴──────────────────────┴──────────────────────┴──────┴────────┘
"""

import importlib.util
from dataclasses import dataclass
from typing import Optional

from polypolars import polars_factory

HAS_POLARS = importlib.util.find_spec("polars") is not None


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
