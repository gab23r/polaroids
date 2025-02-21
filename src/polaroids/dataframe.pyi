from collections.abc import Mapping
from typing import (
    Any,
    Collection,
    Generic,
    Iterable,
    Iterator,
    Literal,
    Self,
    Sequence,
    TypeVar,
    overload,
)

import polars as pl
from polars.dependencies import numpy as np
from polars.polars import Expr
from polars._typing import (
    ColumnNameOrSelector,
    IntoExprColumn,
    IntoExpr,
    UniqueKeepStrategy,
    FillNullStrategy
)


S = TypeVar("S", bound=Mapping)

class DataFrame(pl.DataFrame, Generic[S]):
    """No-op class to make Polars DataFrames generics."""

    def validate(self: Self) -> Self: ...

    @overload
    def rows(self, *, named: Literal[False] = ...) -> list[tuple[Any, ...]]: ...
    @overload
    def rows(self, *, named: Literal[True]) -> list[S]: ...
    def rows( # type: ignore
        self, *, named: bool = False
    ) -> list[tuple[Any, ...]] | list[S]: ...

    @overload
    def row(
        self,
        index: int | None = ...,
        *,
        by_predicate: Expr | None = ...,
        named: Literal[False] = ...,
    ) -> tuple[Any, ...]: ...
    @overload
    def row(
        self,
        index: int | None = ...,
        *,
        by_predicate: Expr | None = ...,
        named: Literal[True],
    ) -> S: ...
    def row( # type: ignore
        self,
        index: int | None = None,
        *,
        by_predicate: Expr | None = None,
        named: bool = False,
    ) -> tuple[Any, ...] | S: ...

    @overload
    def iter_rows(
        self, *, named: Literal[False] = ..., buffer_size: int = ...
    ) -> Iterator[tuple[Any, ...]]: ...
    @overload
    def iter_rows(
        self, *, named: Literal[True], buffer_size: int = ...
    ) -> Iterator[S]: ...
    def iter_rows( # type: ignore
        self, *, named: bool = False, buffer_size: int = 512
    ) -> Iterator[tuple[Any, ...]] | Iterator[S]: ...


    @overload
    def rows_by_key(
        self,
        key: ColumnNameOrSelector | Sequence[ColumnNameOrSelector],
        *,
        named: Literal[False] = ...,
        include_key: bool = ...,
        unique: Literal[False] = ...,
    ) -> dict[Any, list[Any]]: ...
    @overload
    def rows_by_key(
        self,
        key: ColumnNameOrSelector | Sequence[ColumnNameOrSelector],
        *,
        named: Literal[False] = ...,
        include_key: bool = ...,
        unique: Literal[True],
    ) -> dict[Any, Any]: ...
    @overload
    def rows_by_key(
        self,
        key: ColumnNameOrSelector | Sequence[ColumnNameOrSelector],
        *,
        named: Literal[True],
        include_key: bool = ...,
        unique: Literal[False] = ...,
    ) -> dict[Any, list[S]]: ...
    @overload
    def rows_by_key(
        self,
        key: ColumnNameOrSelector | Sequence[ColumnNameOrSelector],
        *,
        named: Literal[True],
        include_key: bool = ...,
        unique: Literal[True],
    ) -> dict[Any, S]: ...
    def rows_by_key( # type: ignore
        self,
        key: ColumnNameOrSelector | Sequence[ColumnNameOrSelector],
        *,
        named: bool = False,
        include_key: bool = False,
        unique: bool = False,
    ) -> dict[Any, Any]: ...


    def head(self: Self, n: int = 5) -> Self: ...
    def limit(self: Self, n: int = 5) -> Self: ...
    def filter(
        self: Self,
        *predicates: (
            IntoExprColumn
            | Iterable[IntoExprColumn]
            | bool
            | list[bool]
            | np.ndarray[Any, Any]
        ),
        **constraints: Any,
    ) -> Self: ...
    def slice(self, offset: int, length: int | None = None) -> Self: ...
    def take(self: Self, indices: IntoExprColumn) -> Self: ...
    def sort(
        self,
        by: IntoExpr | Iterable[IntoExpr],
        *more_by: IntoExpr,
        descending: bool | Sequence[bool] = False,
        nulls_last: bool | Sequence[bool] = False,
        multithreaded: bool = True,
        maintain_order: bool = False,
    ) -> Self: ...
    def drop_nulls(
        self,
        subset: ColumnNameOrSelector | Collection[ColumnNameOrSelector] | None = None,
    ) -> Self: ...
    def unique(
        self,
        subset: ColumnNameOrSelector | Collection[ColumnNameOrSelector] | None = None,
        *,
        keep: UniqueKeepStrategy = "any",
        maintain_order: bool = False,
    ) -> Self: ...
    def fill_null(
        self,
        value: Any | Expr | None = None,
        strategy: FillNullStrategy | None = None,
        limit: int | None = None,
        *,
        matches_supertype: bool = True,
    ) -> DataFrame: ...
    def fill_nan(self, value: Expr | int | float | None) -> DataFrame: ...
