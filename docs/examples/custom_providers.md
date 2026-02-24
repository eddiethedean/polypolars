# Example: Custom providers

You can pass any [polyfactory](https://github.com/litestar-org/polyfactory) arguments through `build_dataframe` or `build_dicts` to customize generation.

```python
from dataclasses import dataclass
from polypolars import polars_factory

@polars_factory
@dataclass
class User:
    id: int
    name: str
    email: str

# Pass polyfactory kwargs through build_dataframe (e.g. for variable-length lists
# use __randomize_collection_length__; for custom factories see polyfactory docs).
df = User.build_dataframe(size=50)
```

Example output (data varies per run):

```
shape: (5, 3)
┌──────┬──────────────────────┬──────────────────────┐
│ id   ┆ name                 ┆ email                │
│ ---  ┆ ---                  ┆ ---                  │
│ i64  ┆ str                  ┆ str                  │
╞══════╪══════════════════════╪══════════════════════╡
│ 2276 ┆ zoGWwQKOrLAVoIveTdwi ┆ ACtwdvvYzYMNeTsPgAkP │
│ 6642 ┆ nnaBxCjpasujWhHcDrcz ┆ ICpTBbqCtEmyFDstckhh │
│ 73   ┆ leHpNFonTtIRsBEbknLf ┆ BPpdQKEjLUxVWApvecnq │
└──────┴──────────────────────┴──────────────────────┘
```

For more control, subclass the generated factory and override field handlers, or use a dedicated `PolarsFactory` subclass with `__model__` and custom `*_handler` methods (see polyfactory docs).
