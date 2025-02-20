from collections.abc import Mapping
from typing import (
    Generic,
    Literal,
    Self,
    TypeVar,
    overload,
)
import polars as pl

S = TypeVar("S", bound=Mapping)

class DataFrame(pl.DataFrame, Generic[S]):
    """No-op class to make Polars DataFrames generics."""

    def validate(self: Self) -> Self: ...

    @overload
    def rows(self, *, named: Literal[True]) -> list[S]: ...  # type: ignore

