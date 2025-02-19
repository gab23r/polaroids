from collections.abc import Mapping
from functools import cached_property
from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    Self,
    TypeVar,
    overload,
    get_type_hints,
)
import polars as pl

S = TypeVar("S", bound=Mapping)


class DataFrame(pl.DataFrame, Generic[S]):
    """No-op class to make Polars DataFrames generics."""

    def __init__(self, data: pl.DataFrame) -> None:
        super().__init__(data)

    def validate(self: Self) -> Self:
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
            name: pl.DataType.from_python(annotation)
            for name, annotation in annotations.items()
        }
        return pl.Schema(converted)

    # --- typing overloads ---
    @overload
    def rows(self, *, named: Literal[False] = ...) -> list[tuple[Any, ...]]: ...

    @overload
    def rows(self, *, named: Literal[True]) -> list[S]: ...

    def rows(  # type: ignore
        self, *, named: bool = False
    ) -> list[tuple[Any, ...]] | list[S]:
        return super().rows(named=named)  # type: ignore
