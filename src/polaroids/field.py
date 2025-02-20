"""Field module."""

from typing import (
    Callable,
    Literal,
    TypedDict,
)
import polars as pl


class Field(TypedDict, total=False):
    """TypedDict representing the configuration for a field in a schema.

    Attributes
    ----------
    primary_key : bool
        Indicates whether the field is a primary key.
    unique : bool
        Indicates whether the field values must be unique.
    sorted : {'descending', 'ascending'}
        Specifies the sorting order for the field.
    coerce : bool
        Indicates whether to coerce the field values to the specified type.
    default : pl.Expr
        The default value for the field.
    checks : list[Callable[[pl.Expr], pl.Expr]]
        A list of validation checks for the field.
    """

    primary_key: bool
    unique: bool
    sorted: Literal["descending", "ascending"]
    coerce: bool
    default: pl.Expr
    checks: list[Callable[[pl.Expr], pl.Expr]]
