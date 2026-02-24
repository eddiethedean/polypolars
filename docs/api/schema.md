# API: Schema

## `infer_schema`

Infer a Polars schema (dict of column name → dtype) from a model type.

```python
infer_schema(model, schema=None, schema_overrides=None)
```

- **schema**: Optional dict or list of column names.
- **schema_overrides**: Optional dict mapping column names to Polars dtypes (e.g. `{"col": pl.Categorical}`).

## `infer_schema_as_struct`

Infer a single `pl.Struct` type from a model (useful for nested columns).

## `python_type_to_polars_type`

Convert a Python type to a Polars DataType.

## `dataclass_to_struct_type`, `pydantic_to_struct_type`, `typed_dict_to_struct_type`

Convert a model type to `pl.Struct`.
