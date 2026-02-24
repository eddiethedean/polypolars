"""Schema inference and conversion logic for Polars."""

from dataclasses import fields as dataclass_fields
from dataclasses import is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional, Tuple, Type, Union, get_args, get_origin

from typing_extensions import get_type_hints

from polypolars.exceptions import SchemaInferenceError, UnsupportedTypeError
from polypolars.protocols import get_polars_module, is_polars_available


def is_optional(type_hint: Type) -> bool:
    """Check if a type hint is Optional (Union with None)."""
    origin = get_origin(type_hint)
    if origin is Union:
        args = get_args(type_hint)
        return type(None) in args
    return False


def unwrap_optional(type_hint: Type) -> Type:
    """Unwrap Optional type to get the inner type."""
    if is_optional(type_hint):
        args = get_args(type_hint)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            return non_none_args[0]
        if non_none_args:
            return non_none_args[0]
    return type_hint


def infer_literal_type(literal_type: Type) -> Type:
    """Infer the base type from a Literal type."""
    origin = get_origin(literal_type)
    if origin is not Literal:
        return literal_type

    args = get_args(literal_type)
    if not args:
        raise SchemaInferenceError(f"Empty Literal type: {literal_type}")

    value_types = [type(arg) for arg in args]
    if len(set(value_types)) == 1:
        return value_types[0]
    if all(t in (int, float) for t in value_types):
        return float if float in value_types else int
    if all(t is str for t in value_types):
        return str
    if all(t is bool for t in value_types):
        return bool
    raise SchemaInferenceError(
        f"Cannot infer unified type from Literal with mixed types: {literal_type}"
    )


def python_type_to_polars_type(python_type: Type, nullable: bool = True) -> Any:
    """Convert a Python type to a Polars DataType.

    Args:
        python_type: The Python type to convert.
        nullable: Whether the field should be nullable (Polars columns are nullable by default).

    Returns:
        Polars DataType instance if Polars is available.

    Raises:
        UnsupportedTypeError: If the type cannot be converted.
    """
    if not is_polars_available():
        raise UnsupportedTypeError(
            "Polars is required for schema inference. Install it with: pip install polars"
        )

    pl = get_polars_module()
    assert pl is not None

    # Handle Optional types
    if is_optional(python_type):
        python_type = unwrap_optional(python_type)

    # Handle Literal types
    origin = get_origin(python_type)
    if origin is Literal:
        python_type = infer_literal_type(python_type)

    origin = get_origin(python_type)
    args = get_args(python_type)

    # Basic types
    type_mapping = {
        str: pl.String,
        int: pl.Int64,
        float: pl.Float64,
        bool: pl.Boolean,
        bytes: pl.Binary,
        bytearray: pl.Binary,
        date: pl.Date,
        datetime: pl.Datetime("us"),
        Decimal: pl.Decimal(scale=0, precision=10),
    }

    if python_type in type_mapping:
        return type_mapping[python_type]

    # Tuple (fixed size) -> Array
    if origin in (tuple, Tuple) and args:
        if len(args) == 2 and args[1] is Ellipsis:
            # Tuple[T, ...] -> List(T)
            inner = python_type_to_polars_type(args[0], nullable=True)
            return pl.List(inner)
        # Tuple[T, T, ...] -> Array(inner, size)
        try:
            inner = python_type_to_polars_type(args[0], nullable=True)
            return pl.Array(inner, len(args))
        except Exception:
            pass

    # List -> List type
    if origin in (list, List):
        if not args:
            raise SchemaInferenceError(f"Cannot infer list element type from {python_type}")
        inner = python_type_to_polars_type(args[0], nullable=True)
        return pl.List(inner)

    # Dict -> List(Struct) (Polars has no Map type; represent as list of key-value structs)
    if origin in (dict, Dict):
        if not args or len(args) < 2:
            raise SchemaInferenceError(f"Cannot infer dict types from {python_type}")
        key_type = python_type_to_polars_type(args[0], nullable=False)
        value_type = python_type_to_polars_type(args[1], nullable=True)
        return pl.List(pl.Struct([pl.Field("key", key_type), pl.Field("value", value_type)]))

    # Dataclass / Pydantic -> Struct
    if is_dataclass(python_type):
        return dataclass_to_struct_type(python_type)

    if hasattr(python_type, "model_fields"):
        return pydantic_to_struct_type(python_type)

    if hasattr(python_type, "__annotations__"):
        try:
            return typed_dict_to_struct_type(python_type)
        except Exception:
            pass

    raise UnsupportedTypeError("Cannot convert to Polars type", type_hint=python_type)


def dataclass_to_struct_type(dataclass_type: Type) -> Any:
    """Convert a dataclass to a Polars Struct type."""
    if not is_polars_available():
        from polypolars.exceptions import PolarsNotAvailableError
        raise PolarsNotAvailableError()

    pl = get_polars_module()
    assert pl is not None

    if not is_dataclass(dataclass_type):
        raise ValueError(f"{dataclass_type} is not a dataclass")

    fields = []
    type_hints = get_type_hints(dataclass_type)

    for field in dataclass_fields(dataclass_type):
        field_type = type_hints.get(field.name, field.type)
        nullable = is_optional(field_type)
        polars_type = python_type_to_polars_type(field_type, nullable=nullable)
        fields.append(pl.Field(field.name, polars_type))

    return pl.Struct(fields)


def pydantic_to_struct_type(model_type: Type) -> Any:
    """Convert a Pydantic model to a Polars Struct type."""
    if not is_polars_available():
        from polypolars.exceptions import PolarsNotAvailableError
        raise PolarsNotAvailableError()

    pl = get_polars_module()
    assert pl is not None

    if not hasattr(model_type, "model_fields"):
        raise ValueError(f"{model_type} is not a Pydantic v2 model")

    fields = []
    for field_name, field_info in model_type.model_fields.items():
        field_type = field_info.annotation
        nullable = not field_info.is_required() or is_optional(field_type)
        polars_type = python_type_to_polars_type(field_type, nullable=nullable)
        fields.append(pl.Field(field_name, polars_type))

    return pl.Struct(fields)


def typed_dict_to_struct_type(typed_dict_type: Type) -> Any:
    """Convert a TypedDict to a Polars Struct type."""
    if not is_polars_available():
        from polypolars.exceptions import PolarsNotAvailableError
        raise PolarsNotAvailableError()

    pl = get_polars_module()
    assert pl is not None

    if not hasattr(typed_dict_type, "__annotations__"):
        raise ValueError(f"{typed_dict_type} does not have type annotations")

    required_keys: Any = getattr(typed_dict_type, "__required_keys__", set())
    fields = []

    for field_name, field_type in typed_dict_type.__annotations__.items():
        nullable = field_name not in required_keys or is_optional(field_type)
        polars_type = python_type_to_polars_type(field_type, nullable=nullable)
        fields.append(pl.Field(field_name, polars_type))

    return pl.Struct(fields)


def infer_schema(
    model: Type,
    schema: Optional[Union[Dict[str, Any], List[str]]] = None,
    schema_overrides: Optional[Dict[str, Any]] = None,
) -> Any:
    """Infer or validate a Polars schema from a model type.

    Args:
        model: The model type (dataclass, Pydantic, TypedDict).
        schema: Optional explicit schema. If dict, used as column name -> Polars dtype.
                If list of strings, column names to include (types inferred from model).

    Returns:
        A dict mapping column names to Polars DataTypes (for pl.DataFrame(schema=...)).
    """
    if not is_polars_available():
        raise SchemaInferenceError("Polars is required for schema inference")

    pl = get_polars_module()
    assert pl is not None

    # Build schema dict from model fields
    if is_dataclass(model):
        type_hints = get_type_hints(model)
        fields_source = [(f.name, type_hints.get(f.name, f.type)) for f in dataclass_fields(model)]
    elif hasattr(model, "model_fields"):
        fields_source = [(k, v.annotation) for k, v in model.model_fields.items()]
    elif hasattr(model, "__annotations__"):
        fields_source = list(model.__annotations__.items())
    else:
        raise SchemaInferenceError(f"Cannot infer schema from {model}")

    inferred = {}
    for name, py_type in fields_source:
        if schema_overrides and name in schema_overrides:
            inferred[name] = schema_overrides[name]
        else:
            inferred[name] = python_type_to_polars_type(py_type, nullable=is_optional(py_type))

    if schema is None:
        return inferred

    if isinstance(schema, dict):
        return schema

    if isinstance(schema, list):
        for col in schema:
            if col not in inferred:
                raise SchemaInferenceError(f"Column '{col}' not found in model {model}")
        return {k: inferred[k] for k in schema if k in inferred}

    return inferred


def infer_schema_as_struct(model: Type) -> Any:
    """Infer Polars Struct type from model (for nested columns)."""
    if is_dataclass(model):
        return dataclass_to_struct_type(model)
    if hasattr(model, "model_fields"):
        return pydantic_to_struct_type(model)
    if hasattr(model, "__annotations__"):
        return typed_dict_to_struct_type(model)
    raise SchemaInferenceError(f"Cannot infer schema from {model}")
