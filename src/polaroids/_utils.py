"""Utils module."""

from collections.abc import Mapping
from typing import TypeVar, get_type_hints
import polars as pl
from polaroids.exceptions import ValidationError

S = TypeVar("S", bound=Mapping)


def get_nullable_cols(typeddict: type[S]):
    return [
        name
        for name, dtype in get_type_hints(typeddict).items()
        if type(None) in getattr(dtype, "__args__", [])
    ]


def assert_schema_equal(left: pl.Schema, right: pl.Schema):
    # source: https://github.com/pola-rs/polars/issues/21215
    if left != right:
        l_names = set(left.names())
        r_names = set(right.names())

        l_exclusive = l_names - r_names
        r_exclusive = r_names - l_names
        different_types = {name for name in (l_names & r_names) if not left[name].is_(right[name])}

        message = ""

        if r_exclusive:
            message += f"left missing columns: {sorted(r_exclusive)}\n"
        if l_exclusive:
            message += f"right missing columns: {sorted(l_exclusive)}\n"
        if different_types:
            message += "columns have different types:\n"
            for name in sorted(different_types):
                l_type = left[name]
                r_type = right[name]
                message += f"  {name}: {l_type} != {r_type}\n"
        if message:
            raise ValidationError(message)
