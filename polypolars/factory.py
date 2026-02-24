"""PolarsFactory class for generating Polars DataFrames."""

import functools
from abc import ABC
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from polyfactory.factories import DataclassFactory

try:
    from pydantic import BaseModel
except ImportError:
    BaseModel = None  # type: ignore[assignment, misc]

from polypolars.exceptions import PolarsNotAvailableError
from polypolars.protocols import is_polars_available
from polypolars.schema import infer_schema

T = TypeVar("T")


class PolarsFactory(DataclassFactory[T], ABC):
    """Factory for generating Polars DataFrames from models.

    Works with dataclasses, Pydantic models, and TypedDicts.
    """

    __is_base_factory__ = True

    @classmethod
    def build_dataframe(
        cls,
        size: int = 10,
        schema: Optional[Union[Dict[str, Any], List[str]]] = None,
        schema_overrides: Optional[Dict[str, Any]] = None,
        chunk_size: Optional[int] = None,
        **kwargs: Any,
    ) -> Any:
        """Build a Polars DataFrame with generated data.

        Args:
            size: Number of rows to generate.
            schema: Optional explicit schema (dict of column name -> Polars dtype, or list of column names).
            schema_overrides: Optional per-column dtype overrides (e.g. {"col": pl.Categorical}).
            chunk_size: If set, build in chunks and concat (reduces memory for very large size).
            **kwargs: Additional keyword arguments passed to the factory.

        Returns:
            A Polars DataFrame with generated data.
        """
        if not is_polars_available():
            raise PolarsNotAvailableError()

        import polars as pl

        inferred_schema = infer_schema(cls.__model__, schema, schema_overrides=schema_overrides)
        if chunk_size is not None and chunk_size > 0 and size > chunk_size:
            chunks = []
            remaining = size
            while remaining > 0:
                n = min(chunk_size, remaining)
                data = cls.build_dicts(size=n, **kwargs)
                chunks.append(pl.DataFrame(data, schema=inferred_schema))
                remaining -= n
            return pl.concat(chunks)
        data = cls.build_dicts(size=size, **kwargs)
        return pl.DataFrame(data, schema=inferred_schema)

    @classmethod
    def build_lazy_dataframe(
        cls,
        size: int = 10,
        schema: Optional[Union[Dict[str, Any], List[str]]] = None,
        schema_overrides: Optional[Dict[str, Any]] = None,
        chunk_size: Optional[int] = None,
        **kwargs: Any,
    ) -> Any:
        """Build a Polars LazyFrame with generated data.

        Args:
            size: Number of rows to generate.
            schema: Optional explicit schema.
            schema_overrides: Optional per-column dtype overrides.
            chunk_size: If set, build DataFrame in chunks then convert to LazyFrame.
            **kwargs: Additional keyword arguments passed to the factory.

        Returns:
            A Polars LazyFrame with generated data.
        """
        df = cls.build_dataframe(
            size=size, schema=schema, schema_overrides=schema_overrides,
            chunk_size=chunk_size, **kwargs
        )
        return df.lazy()

    @classmethod
    def build_dicts(
        cls,
        size: int = 10,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """Build a list of dictionaries with generated data.

        Use create_dataframe_from_dicts to convert to a DataFrame later.
        """
        instances = cls.batch(size=size, **kwargs)
        dicts = []
        for instance in instances:
            if is_dataclass(instance):
                dicts.append(asdict(instance))  # type: ignore[arg-type]
            elif BaseModel is not None and isinstance(instance, BaseModel):
                dicts.append(instance.model_dump())
            elif isinstance(instance, dict):
                dicts.append(instance)
            else:
                try:
                    dicts.append(dict(instance))  # type: ignore[call-overload]
                except (TypeError, ValueError):
                    dicts.append(instance.__dict__)
        return dicts

    @classmethod
    def create_dataframe_from_dicts(
        cls,
        data: List[Dict[str, Any]],
        schema: Optional[Union[Dict[str, Any], List[str]]] = None,
    ) -> Any:
        """Convert pre-generated dictionary data to a Polars DataFrame."""
        if not is_polars_available():
            raise PolarsNotAvailableError()

        import polars as pl

        inferred_schema = infer_schema(cls.__model__, schema)
        return pl.DataFrame(data, schema=inferred_schema)


def build_polars_dataframe(
    model: Type[T],
    size: int = 10,
    schema: Optional[Union[Dict[str, Any], List[str]]] = None,
    **kwargs: Any,
) -> Any:
    """Convenience function to build a DataFrame without creating a factory class."""
    factory_class = type(
        f"{model.__name__}Factory",
        (PolarsFactory,),
        {"__model__": model},
    )
    return factory_class.build_dataframe(size=size, schema=schema, **kwargs)  # type: ignore[attr-defined]


def polars_factory(cls: Type[T]) -> Type[T]:
    """Decorator to add factory methods directly to a model class."""

    if BaseModel is not None and isinstance(cls, type) and issubclass(cls, BaseModel):
        try:
            from polyfactory.factories.pydantic_factory import ModelFactory as PydanticModelFactory

            class _PydanticPolarsFactory(PydanticModelFactory):
                __is_base_factory__ = True

                @classmethod
                def build_dataframe(cls, *args: Any, **kwargs: Any) -> Any:
                    return PolarsFactory.build_dataframe.__func__(cls, *args, **kwargs)  # type: ignore[attr-defined]

                @classmethod
                def build_dicts(cls, *args: Any, **kwargs: Any) -> Any:
                    return PolarsFactory.build_dicts.__func__(cls, *args, **kwargs)  # type: ignore[attr-defined]

                @classmethod
                def create_dataframe_from_dicts(cls, *args: Any, **kwargs: Any) -> Any:
                    return PolarsFactory.create_dataframe_from_dicts.__func__(cls, *args, **kwargs)  # type: ignore[attr-defined]

                @classmethod
                def build_lazy_dataframe(cls, *args: Any, **kwargs: Any) -> Any:
                    return PolarsFactory.build_lazy_dataframe.__func__(cls, *args, **kwargs)  # type: ignore[attr-defined]

            factory_class = type(
                f"_{cls.__name__}Factory",
                (_PydanticPolarsFactory,),
                {"__model__": cls, "__is_base_factory__": False},
            )
        except ImportError:
            factory_class = type(
                f"_{cls.__name__}Factory",
                (PolarsFactory,),
                {"__model__": cls, "__is_base_factory__": False},
            )
    else:
        factory_class = type(
            f"_{cls.__name__}Factory",
            (PolarsFactory,),
            {"__model__": cls, "__is_base_factory__": False},
        )

    @classmethod  # type: ignore[misc]
    @functools.wraps(PolarsFactory.build_dataframe)
    def build_dataframe(
        model_cls: Type[T],
        size: int = 10,
        schema: Optional[Union[Dict[str, Any], List[str]]] = None,
        **kwargs: Any,
    ) -> Any:
        return factory_class.build_dataframe(size=size, schema=schema, **kwargs)  # type: ignore[attr-defined]

    @classmethod  # type: ignore[misc]
    @functools.wraps(PolarsFactory.build_dicts)
    def build_dicts(
        model_cls: Type[T],
        size: int = 10,
        **kwargs: Any,
    ) -> Any:
        return factory_class.build_dicts(size=size, **kwargs)  # type: ignore[attr-defined]

    @classmethod  # type: ignore[misc]
    @functools.wraps(PolarsFactory.create_dataframe_from_dicts)
    def create_dataframe_from_dicts(
        model_cls: Type[T],
        data: List[Dict[str, Any]],
        schema: Optional[Union[Dict[str, Any], List[str]]] = None,
    ) -> Any:
        return factory_class.create_dataframe_from_dicts(data, schema=schema)  # type: ignore[attr-defined]

    @classmethod  # type: ignore[misc]
    @functools.wraps(PolarsFactory.build_lazy_dataframe)
    def build_lazy_dataframe(
        model_cls: Type[T],
        size: int = 10,
        schema: Optional[Union[Dict[str, Any], List[str]]] = None,
        chunk_size: Optional[int] = None,
        **kwargs: Any,
    ) -> Any:
        return factory_class.build_lazy_dataframe(size=size, schema=schema, chunk_size=chunk_size, **kwargs)  # type: ignore[attr-defined]

    cls.build_dataframe = build_dataframe  # type: ignore[attr-defined]
    cls.build_dicts = build_dicts  # type: ignore[attr-defined]
    cls.create_dataframe_from_dicts = create_dataframe_from_dicts  # type: ignore[attr-defined]
    cls.build_lazy_dataframe = build_lazy_dataframe  # type: ignore[attr-defined]
    cls._polypolars_factory = factory_class  # type: ignore[attr-defined]

    return cls
