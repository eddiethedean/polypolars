# API: Factory

## `polars_factory`

Decorator that adds `build_dataframe`, `build_dicts`, `create_dataframe_from_dicts`, and `build_lazy_dataframe` to a model class.

## `PolarsFactory`

Base factory class. Subclass with `__model__ = YourModel`.

### Methods

- **build_dataframe**(size=10, schema=None, schema_overrides=None, chunk_size=None, **kwargs) → pl.DataFrame
- **build_dicts**(size=10, **kwargs) → List[Dict]
- **create_dataframe_from_dicts**(data, schema=None) → pl.DataFrame
- **build_lazy_dataframe**(size=10, schema=None, schema_overrides=None, chunk_size=None, **kwargs) → pl.LazyFrame

## `build_polars_dataframe`

Function to build a DataFrame without a factory class:

```python
build_polars_dataframe(Model, size=100, schema=None, schema_overrides=None, **kwargs)
```
