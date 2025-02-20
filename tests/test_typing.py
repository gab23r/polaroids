from typing import TypedDict

from polaroids import DataFrame
import polars as pl


class BasicSchema(TypedDict):
    a: int


def test_typing_rows():
    df = pl.DataFrame({"a": [0, 1]})
    basic_df = DataFrame[BasicSchema](df)

    _: BasicSchema = basic_df.rows(named=True)[0]  # to ensure pyright is happy
