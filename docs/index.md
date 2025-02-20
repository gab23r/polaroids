# Polars on Steroids!  

This package provides a generic extension to Polars `DataFrame`, allowing data validation and typing goodies.

## Features
- **Generic DataFrame**: Ensures type safety using Python's `TypedDict`.
- **Data Validation**: Checks that the DataFrame conforms to the expected schema.
- **Custom Checks**: Leverage the power of polars expression to add custom checks.
- **Lightweight**: No dependencies (except polars)!

## Installation

```sh
pip install polaroids
```

## Documentation

📖 **Read the full documentation here:** [Project Documentation](https://gab23r.github.io/polaroids/)

## Get Started

### Defining a Schema
Schemas are defined using Python's `TypedDict`:

```python
from typing import Annotated, TypedDict
from polaroids import DataFrame, Field
import polars as pl

class BasicSchema(TypedDict):
    a: Annotated[int, Field(
        sorted="ascending",
        coerce=True,
        unique=True,
        checks=[lambda d: d.ge(0)],
    )]
    b: int | None

df = pl.DataFrame({"a": [0.0, 1.0], "b": [None, 0]})

DataFrame[BasicSchema](df).validate()
shape: (2, 2)
┌─────┬──────┐
│ a   ┆ b    │
│ --- ┆ ---  │
│ i64 ┆ i64  │
╞═════╪══════╡
│ 0   ┆ null │
│ 1   ┆ 0    │
└─────┴──────┘
```

### Get typing goodies !
Get your TypedDict back when you leave polars ✅:

![alt text](img/typing_completion.png)


### Adding Custom Validations
Extend `DataFrame` and define validation methods prefixed with `check_`:

```python
class BasicSchemaDataFrame(DataFrame[BasicSchema]):
    def check_a_greater_then_b(self) -> None:
        assert self.select((pl.col("a") >= pl.col("b")).all()).item(), "a should be greater the b"

# Example usage
BasicSchemaDataFrame(df).validate() # Passes validation

# This will raise an AssertionError
(
    pl.DataFrame({"a": [5, 6], "b": [None, 10]})
    .pipe(BasicSchemaDataFrame)
    .validate() # This will raise 💣 !
)
```


