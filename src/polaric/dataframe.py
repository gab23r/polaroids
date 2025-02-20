"""Main module."""

from collections.abc import Mapping
from functools import cached_property
from typing import (
    Callable,
    Generic,
    Self,
    TypeVar,
)
import polars as pl
from polaric import _utils
from polaric.exceptions import ValidationError
from polaric.field import Field


S = TypeVar("S", bound=Mapping)


class _Metadata(Field):
    column: str
    nullable: bool


class DataFrame(pl.DataFrame, Generic[S]):
    """No-op class to make Polars DataFrames generics."""

    def validate(self: Self) -> Self:
        """Validate the dataframe."""
        # Coerce
        if coerce_cols := self._metadata.filter(pl.col("coerce"))["column"].to_list():
            self._df = self.cast(
                {c: dtype for c, dtype in self._schema.items() if c in coerce_cols}
            )._df

        _utils.assert_schema_equal(self._schema, self.schema)

        # Nullable
        if non_nullable_cols := self._metadata.filter(~pl.col("nullable"))["column"].to_list():
            is_null = (
                self.select(pl.col(non_nullable_cols).is_null().any())
                .transpose(include_header=True, column_names=["is_null"])
                .filter(pl.col("is_null"))
                .get_column("column")
                .to_list()
            )
            if is_null:
                raise ValidationError(f"The following columns contains nulls: {is_null}.")

        # Uniqueness
        if unique_cols := self._metadata.filter(pl.col("unique"))["column"].to_list():
            is_duplicated = (
                self.select(pl.col(unique_cols).is_duplicated().any())
                .transpose(include_header=True, column_names=["is_duplicated"])
                .filter(pl.col("is_duplicated"))
                .get_column("column")
                .to_list()
            )
            if is_duplicated:
                raise ValidationError(
                    f"The following columns must be unique but contain duplicates: {is_duplicated}."
                )

        # Primary key
        if pk_cols := self._metadata.filter(pl.col("primary_key"))["column"].to_list():
            df_duplicated = self.select(pk_cols).filter(pl.struct(pk_cols).is_duplicated())
            if df_duplicated.height:
                raise ValidationError(f"Primary key constraint violated:\n{df_duplicated}.")

        # Is sorted
        for descending, columns in (
            self._metadata.filter(pl.col("sorted").is_not_null())
            .group_by(descending=pl.col("sorted").eq("descending"))
            .agg("column")
            .iter_rows()
        ):
            for column in columns:
                if not self.get_column(column).is_sorted(descending=descending):
                    raise ValidationError(
                        f"Column {column!r} is not sorted as expected (descending={descending})."
                    )
            self._df = self.with_columns(pl.col(columns).set_sorted(descending=descending))._df

        # Custom checks
        for name, f in self.__class__.__dict__.items():
            if name.startswith("check_") and isinstance(f, Callable):
                f(self)
        return self

    @cached_property
    def _typeddict(self) -> type[S]:
        df_class = getattr(self, "__orig_class__", type(self).__orig_bases__[0])  # type: ignore
        schema_class = df_class.__args__[0]
        return schema_class

    @cached_property
    def _schema(self) -> pl.Schema:
        return _utils.typeddict_to_polats_schema(self._typeddict)

    @cached_property
    def _metadata(self) -> "DataFrame[_Metadata]":
        return DataFrame[_Metadata](
            pl.from_dicts(
                [
                    {"column": col, "nullable": col in _utils.get_nullable_cols(self._typeddict)}
                    | getattr(self._typeddict.__annotations__[col], "__metadata__", [{}])[0]
                    for col in self.columns
                ],
                schema=_utils.typeddict_to_polats_schema(_Metadata),
            ).select("column", "nullable", pl.exclude("column", "nullable"))
        )
