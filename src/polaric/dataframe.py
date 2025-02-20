"""Main module."""

from collections.abc import Mapping
from functools import cached_property
from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    Self,
    TypeVar,
    get_type_hints,
)
import polars as pl
from polars._typing import PolarsDataType

S = TypeVar("S", bound=Mapping)


class DataFrame(pl.DataFrame, Generic[S]):
    """No-op class to make Polars DataFrames generics."""

    def __init__(self, data: pl.DataFrame) -> None:
        super().__init__(data)

    def validate(self: Self) -> Self:
        """Validate the dataframe."""
        assert self._expected_schema == self.schema, "Schema mismatch"
        for name, f in self.__class__.__dict__.items():
            if name.startswith("check_") and isinstance(f, Callable):
                f(self)
        return self

    @cached_property
    def _expected_schema(self) -> pl.Schema:
        # Access the type arguments from the class level

        __orig_class__ = getattr(self, "__orig_class__", type(self).__orig_bases__[0])  # type: ignore
        annotations = get_type_hints(__orig_class__.__args__[0])

        converted = {
            name: _annotation_to_polars_dtype(annotation)
            for name, annotation in annotations.items()
            if name != "_checks"
        }
        return pl.Schema(converted)


def _annotation_to_polars_dtype(annotation: Any) -> PolarsDataType:
    if hasattr(annotation, "__origin__") and annotation.__origin__ is Literal:
        all_string = all(isinstance(arg, str) for arg in annotation.__args__)
        assert all_string
        return pl.Enum(annotation.__args__)

    return pl.DataType.from_python(annotation)
