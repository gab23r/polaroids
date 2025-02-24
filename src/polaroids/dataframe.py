"""Main module."""

from collections.abc import Mapping
from functools import cached_property
from typing import (
    Generic,
    Self,
    TypeVar,
)
import polars as pl
from polaroids import _utils
from polaroids._parse_types import typeddict_to_polats_schema
from polaroids.exceptions import ValidationError
from polaroids.field import Field


S = TypeVar("S", bound=Mapping)


class _Metadata(Field):
    column: str
    nullable: bool


class DataFrame(pl.DataFrame, Generic[S]):
    """A generic Polars DataFrame with schema validation.

    This class extends `polars.DataFrame` to support schema validation using
    Python's type annotations and metadata. It ensures that the DataFrame
    conforms to a specified schema, enforcing constraints such as sorting,
    uniqueness, and custom validation checks.

    Parameters
    ----------
    df : polars.DataFrame
        The input DataFrame to be validated.

    Type Parameters
    ---------------
    S : TypedDict
        The schema definition as a `TypedDict`, where fields can have metadata
        such as sorting, uniqueness, coercion, and validation checks.

    Methods
    -------
    validate()
        Validates the DataFrame against the expected schema.

    Example
    -------
    ```python
    from typing import Annotated, TypedDict
    from polaroids import DataFrame, Field
    from polaroids.types import int32
    import polars as pl


    class BasicSchema(TypedDict):
        a: Annotated[
            int32,
            Field(
                sorted="ascending",
                coerce=True,
                unique=True,
                checks=[lambda d: d.ge(0)],  # Ensures values are non-negative
            ),
        ]
        b: int | None  # Optional integer column


    df = pl.DataFrame({"a": [0.0, 1.0], "b": [None, 0]})
    validated_df = DataFrame[BasicSchema](df).validate()
    ```

    The `validate()` method ensures that:
    - The schema of `df` matches the TypedDict (with possible coercion).
    - Column `a` is sorted in ascending order.
    - Column `a` only contains non-negative values.
    - Column `a` has unique values.
    - Column `b` allows `None` values.

    Raises
    ------
    ValidationError
        If the DataFrame does not conform to the expected schema.
    """

    def __init__(self, df: pl.DataFrame):
        super().__init__(df)

    def __getattribute__(self, name: str):
        """Dynamically delegate attribute access to the underlying `polars.DataFrame`.

        This method intercepts attribute lookups that are not found on `DataFrame`
        and attempts to retrieve them from the `polars.DataFrame` superclass, the restult is converted back into an instance
        of this `DataFrame` subclass.

        We intercept only on subset of polars.DataFrame methods, we intercept only methods that might not change the schema.
        """
        if name in [
            "head",
            "limit",
            "filter",
            "slice",
            "sort",
            "drop_nulls",
            "unique",
            "fill_null",
            "fill_nan",
            "with_columns",
            "select",
            "cast",
        ]:
            attr = getattr(super(), name)  # Get the attribute from `pl.DataFrame`

            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)  # Call the method
                new = self.__class__(result)
                setattr(new, "__orig_class__", getattr(self, "__orig_class__", None))
                return new

            return wrapper

        return super().__getattribute__(name)  # Get the original method from `pl.DataFrame`

    def validate(self: Self) -> Self:
        """Validate the dataframe based on the annotations of the TypedDict.

        This function performs various validation checks, including:

        - **Schema equality**: Ensures that the DataFrame matches the expected schema.
        - **Primary key uniqueness**: Verifies that primary key columns contain unique values.
        - **Unique values**: Checks for unique constraints on specific columns.
        - **Nullable columns**: Ensures that required columns do not contain null values.
        - **Sortedness**: Validates whether specified columns are sorted in the expected order.
        - **Custom checks**: Applies user-defined validation functions.

        Returns
        -------
            Self: The validated DataFrame.

        Raises
        ------
            ValidationError: If any validation check fails.
        """
        # Coerce
        if coerce_cols := self._metadata.filter(pl.col("coerce"))["column"].to_list():
            self = self.cast({c: dtype for c, dtype in self._schema.items() if c in coerce_cols})  # type: ignore

        _utils.assert_schema_equal(self._schema, self.schema)

        # Nullable
        if non_nullable_cols := self._metadata.filter(~pl.col("nullable"))["column"].to_list():
            if is_null := (
                self.select(pl.col(non_nullable_cols).is_null().any())
                .transpose(include_header=True, column_names=["is_null"])
                .filter(pl.col("is_null"))
                .get_column("column")
                .to_list()
            ):
                raise ValidationError(f"The following columns contains nulls: {is_null}.")

        # Uniqueness
        if unique_cols := self._metadata.filter(pl.col("unique"))["column"].to_list():
            if is_duplicated := (
                self.select(pl.col(unique_cols).is_duplicated().any())
                .transpose(include_header=True, column_names=["is_duplicated"])
                .filter(pl.col("is_duplicated"))
                .get_column("column")
                .to_list()
            ):
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
            self = self.with_columns(pl.col(columns).set_sorted(descending=descending))  # type: ignore

        # Custom checks
        for column, checks in (
            self._metadata.select("column", "checks").filter(pl.col("checks").is_not_null()).rows()
        ):
            result = self.select(
                [check(pl.col(column)).alias(str(i)) for i, check in enumerate(checks)]
            )
            for i, check_ok in result.select(pl.all().all()).row(0, named=True).items():
                if not check_ok:
                    df_failure = self.filter(result.get_column(i))
                    raise ValidationError(
                        f"Check number {i} on column {column!r} fails:\n{df_failure}."
                    )

        return self

    @cached_property
    def _typeddict(self) -> type[S]:
        df_class = getattr(self, "__orig_class__", type(self).__orig_bases__[0])  # type: ignore
        schema_class = df_class.__args__[0]
        return schema_class

    @cached_property
    def _schema(self) -> pl.Schema:
        return typeddict_to_polats_schema(self._typeddict)

    @cached_property
    def _metadata(self) -> pl.DataFrame:
        return pl.from_dicts(
            [
                {"column": col, "nullable": col in _utils.get_nullable_cols(self._typeddict)}
                | getattr(self._typeddict.__annotations__[col], "__metadata__", [{}])[0]
                for col in self.columns
            ],
            schema={
                "primary_key": pl.Boolean,
                "unique": pl.Boolean,
                "sorted": pl.Enum(categories=["descending", "ascending"]),
                "coerce": pl.Boolean,
                "default": pl.Object,
                "checks": pl.Object,
                "column": pl.String,
                "nullable": pl.Boolean,
            },
        ).select("column", "nullable", pl.exclude("column", "nullable"))
