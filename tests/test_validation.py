from typing import Annotated, Self, TypedDict

import pytest
from polaroids import DataFrame
import polars as pl

from polaroids import Field
from polaroids.exceptions import ValidationError
unique=True

class BasicSchema(TypedDict):
    a: int


def test_schema_validation():
    df = pl.DataFrame({"a": [0, 1]})
    DataFrame[BasicSchema](df).validate()


def test_custom_validation():
    class BasicSchemaDataFrame(DataFrame[BasicSchema]):
        """Extended DataFrame with additional validation logic."""

        def check_a_is_positive(self: Self) -> Self:
            # Call the original validate method
            assert self.select(pl.col("a").ge(0).all()).item(), (
                "a contains negative values"
            )
            return self

    df = pl.DataFrame({"a": [0, 1]})
    BasicSchemaDataFrame(df).validate()

    with pytest.raises(AssertionError, match="contains negative values"):
        df = pl.DataFrame({"a": [-1, 1]})
        BasicSchemaDataFrame(df).validate()

def test_is_unique():

    class Schema(TypedDict):
        a: Annotated[int, Field(unique=True)]
        b: int

    df = pl.DataFrame({"a": [0, 1], "b": [0, 1]})
    DataFrame[Schema](df).validate()

    with pytest.raises(ValidationError, match="contain duplicates"):
        df = pl.DataFrame({"a": [0, 0], "b": [0, 1]})
        DataFrame[Schema](df).validate()