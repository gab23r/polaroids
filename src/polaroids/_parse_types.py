# Copied from Polars (https://github.com/pola-rs/polars/blob/93bfd2c796966aaa0a46a22d51c1f147111e03b8/py-polars/polars/datatypes/_parse.py)
# Licensed under MIT License

# We copied the logic of parsing the python datatype, and our own types in the logic.

import enum
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal as PyDecimal
from inspect import isclass
from types import NoneType, UnionType
from typing import (
    Any,
    Literal,
    NoReturn,
    TypeVar,
    Union,
    get_args,
    get_type_hints,
    is_typeddict,
)

import polars as pl
from polars._typing import PolarsDataType, PythonDataType

UnionTypeOld = type(Union[int, str])


polars_dtype_mapping = {
    "polaroids.types.int8": pl.Int8,
    "polaroids.types.int16": pl.Int16,
    "polaroids.types.int32": pl.Int32,
    "polaroids.types.int64": pl.Int64,
    "polaroids.types.uint8": pl.UInt8,
    "polaroids.types.uint16": pl.UInt16,
    "polaroids.types.uint32": pl.UInt32,
    "polaroids.types.uint64": pl.UInt64,
    "polaroids.types.float32": pl.Float32,
    "polaroids.types.float64": pl.Float64,
}
S = TypeVar("S", bound=Mapping)


def typeddict_to_polars_schema(typeddict: type[S]) -> pl.Schema:
    converted = {
        name: parse_into_dtype(annotation) for name, annotation in get_type_hints(typeddict).items()
    }
    return pl.Schema(converted)


def parse_into_dtype(input: Any) -> PolarsDataType:
    """
    Parse an input into a Polars data type.

    Raises
    ------
    TypeError
        If the input cannot be parsed into a Polars data type.
    """
    if isinstance(input, (UnionType, UnionTypeOld)):
        return _parse_union_type_into_dtype(input)
    elif is_typeddict(input):
        return pl.Struct(typeddict_to_polars_schema(input))
    else:
        return parse_py_type_into_dtype(input)


def _parse_union_type_into_dtype(input: Any) -> PolarsDataType:
    """
    Parse a union of types into a Polars data type.

    Unions of multiple non-null types (e.g. `int | float`) are not supported.

    Parameters
    ----------
    input
        A union type, e.g. `str | None` (new syntax) or `Union[str, None]` (old syntax).
    """
    # Strip "optional" designation - Polars data types are always nullable
    inner_types = [tp for tp in get_args(input) if tp is not NoneType]

    if len(inner_types) != 1:
        _raise_on_invalid_dtype(input)

    input = inner_types[0]
    return parse_into_dtype(input)


def parse_py_type_into_dtype(input: PythonDataType | type[object]) -> PolarsDataType:
    """Convert Python data type to Polars data type."""
    if input is int:
        return pl.Int64()
    elif input is float:
        return pl.Float64()
    elif input is str:
        return pl.String()
    elif input is bool:
        return pl.Boolean()
    elif isinstance(input, type) and issubclass(input, datetime):  # type: ignore[redundant-expr]
        return pl.Datetime("us")
    elif isinstance(input, type) and issubclass(input, date):  # type: ignore[redundant-expr]
        return pl.Date()
    elif input is timedelta:
        return pl.Duration
    elif input is time:
        return pl.Time()
    elif input is PyDecimal:
        return pl.Decimal
    elif input is bytes:
        return pl.Binary()
    elif input is object:
        return pl.Object()
    elif input is NoneType:
        return pl.Null()
    elif input is list or input is tuple:
        return pl.List
    elif isclass(input) and issubclass(input, enum.Enum):
        return pl.Enum(input)
    # this is required as pass through. Don't remove
    elif input == pl.Unknown:
        return pl.Unknown
    # -- Custom code --
    elif hasattr(input, "__origin__") and hasattr(input, "__args__"):
        return _parse_generic_into_dtype(input)
    elif input.__module__ == "polaroids.types":
        return polars_dtype_mapping[str(input)]
    # -- Custom code --

    return pl.Object()


def _parse_generic_into_dtype(input: Any) -> PolarsDataType:
    """Parse a generic type (from typing annotation) into a Polars data type."""
    base_type = input.__origin__
    if base_type in (tuple, list):
        inner_types = input.__args__
        inner_type = inner_types[0]
        if len(inner_types) > 1:
            all_equal = all(t in (inner_type, ...) for t in inner_types)
            if not all_equal:
                _raise_on_invalid_dtype(input)
        inner_dtype = parse_into_dtype(inner_type)
        return pl.List(inner_dtype)

    elif base_type is Literal:
        all_string = all(isinstance(arg, str) for arg in input.__args__)
        if not all_string:
            _raise_on_invalid_dtype(input)
        return pl.Enum(input.__args__)
    return pl.Object()


def _raise_on_invalid_dtype(input: Any) -> NoReturn:
    """Raise an informative error if the input could not be parsed."""
    input_type = input if type(input) is type else f"of type {type(input).__name__!r}"
    input_detail = "" if type(input) is type else f" (given: {input!r})"
    msg = f"cannot parse input {input_type} into Polars data type{input_detail}"
    raise TypeError(msg) from None
