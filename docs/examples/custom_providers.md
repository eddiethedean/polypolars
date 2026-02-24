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

# Use polyfactory's __randomize_collection_length__ for variable-length lists
# or provide custom factories via __set_as_default_factory_key__ and model config.

# Example: limit string length
df = User.build_dataframe(
    size=50,
    __min_length__=1,
    __max_length__=10,
)
```

For more control, subclass the generated factory and override field handlers, or use a dedicated `PolarsFactory` subclass with `__model__` and custom `*_handler` methods (see polyfactory docs).
