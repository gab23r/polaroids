from typing import Annotated, TypedDict

import pytest
from polaroids import DataFrame
import polars as pl

from polaroids import Field
from polaroids.exceptions import ValidationError


class BasicSchema(TypedDict):
    a: int


def test_schema_validation():
    df = pl.DataFrame({"a": [0, 1]})
    DataFrame[BasicSchema](df).validate()


def test_is_unique():
    class Schema(TypedDict):
        a: Annotated[int, Field(unique=True)]
        b: int

    df = pl.DataFrame({"a": [0, 1], "b": [0, 1]})
    DataFrame[Schema](df).validate()

    with pytest.raises(ValidationError, match="contain duplicates"):
        df = pl.DataFrame({"a": [0, 0], "b": [0, 1]})
        DataFrame[Schema](df).validate()


def test_primary_key():
    class Schema(TypedDict):
        a: Annotated[int, Field(primary_key=True)]
        b: Annotated[int, Field(primary_key=True)]

    df = pl.DataFrame({"a": [0, 0], "b": [0, 1]})
    DataFrame[Schema](df).validate()

    with pytest.raises(ValidationError, match="Primary key constraint violated"):
        df = pl.DataFrame({"a": [0, 0], "b": [1, 1]})
        DataFrame[Schema](df).validate()


def test_sorted():
    class Schema(TypedDict):
        a: Annotated[int, Field(sorted="ascending")]
        b: int

    df = pl.DataFrame({"a": [0, 1], "b": [0, 1]})
    DataFrame[Schema](df).validate()

    with pytest.raises(ValidationError, match="is not sorted as expected"):
        df = pl.DataFrame({"a": [1, 0], "b": [0, 1]})
        DataFrame[Schema](df).validate()


def test_coerce():
    class Schema(TypedDict):
        a: Annotated[int, Field(coerce=True)]
        b: int

    df = pl.DataFrame({"a": [0.0, 1.0], "b": [0, 1]})
    validated_df = DataFrame[Schema](df).validate()

    # Verify that coercion occurred
    assert validated_df[["a"]].dtypes[0] == pl.Int64  # Coerced to int


def test_custom_checks():
    class Schema(TypedDict):
        a: Annotated[int, Field(checks=[lambda d: d.ge(0)])]
        b: int

    df = pl.DataFrame({"a": [0, 1], "b": [0, 1]})
    DataFrame[Schema](df).validate()

    with pytest.raises(ValidationError, match="Check number 0 on column 'a' fails"):
        df = pl.DataFrame({"a": [-1, 1], "b": [0, 1]})
        DataFrame[Schema](df).validate()
