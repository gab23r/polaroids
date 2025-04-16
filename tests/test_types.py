from typing import Annotated, Literal, Optional, TypedDict
from polaroids._parse_types import typeddict_to_polars_schema
from polaroids.field import Field
from polaroids.types import int32
import polars as pl


def test_types_parsing() -> None:
    class Schema(TypedDict):
        a: Annotated[list[int32 | None], Field()]
        b: Annotated[list[list[bool]], Field()]
        c: Annotated[list[int] | None, Field()]
        d: Optional[int]
        e: int32 | None
        f: int | None
        g: Literal["a", "b"]
        h: list[Literal["a", "b"] | None]

    expected = pl.Schema(
        {
            "a": pl.List(pl.Int32),
            "b": pl.List(pl.List(pl.Boolean)),
            "c": pl.List(pl.Int64),
            "d": pl.Int64,
            "e": pl.Int32,
            "f": pl.Int64,
            "g": pl.Enum(["a", "b"]),
            "h": pl.List(pl.Enum(["a", "b"])),
        }
    )
    assert expected == typeddict_to_polars_schema(Schema)


def test_types_parsing_nested() -> None:
    class SubSchema(TypedDict):
        a: int | None
        b: str

    class Schema(TypedDict):
        s: list[SubSchema]
        t: int

    expected = pl.Schema(
        [("s", pl.List(pl.Struct({"a": pl.Int64, "b": pl.String}))), ("t", pl.Int64)]
    )
    assert expected == typeddict_to_polars_schema(Schema)
