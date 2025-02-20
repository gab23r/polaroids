"""Field module."""

from typing import (
    Callable,
    Literal,
    TypedDict,
)
import polars as pl


class Field(TypedDict, total=False):
    """TypedDict representing the configuration for a field in a schema.

    Parameters
    ----------
    primary_key
        Indicates whether the field is a primary key.
    unique
        Indicates whether the field values must be unique.
    sorted : {'descending', 'ascending'}
        Specifies the sorting order for the field.
    coerce
        Indicates whether to coerce the field values to the specified type.
    default
        The default value for the field.
    checks
        A list of validation checks for the field.
    """

    primary_key: bool
    unique: bool
    sorted: Literal["descending", "ascending"]
    coerce: bool
    default: pl.Expr
    checks: list[Callable[[pl.Expr], pl.Expr]]
