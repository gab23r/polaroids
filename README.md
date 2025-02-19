# Polars Generic  

This package provides a generic extension to Polars `DataFrame`, allowing schema validation and custom validation logic through class-based definitions.

## Features
- **Generic DataFrame**: Ensures type safety using Python's `TypedDict`.
- **Schema Validation**: Automatically checks that the DataFrame conforms to the expected schema.
- **Custom Validation Hooks**: Define additional validation methods prefixed with `check_`.
- **Improved Typing for Rows**: Provides better type safety for `rows(named=True)`.

## Installation

```sh
pip install polaric
```

## Usage

### Defining a Schema
Schemas are defined using Python's `TypedDict`:

```python
from typing import TypedDict
from polaric import DataFrame, Field
import polars as pl

class BasicSchema(TypedDict):
    a: Annotated[pl.Int64, Field(
        sorted="ascending",
        coerce=True,
        unique=True,
        checks=[lambda d: d.ge(0)],
    )]
    b: str

df = pl.DataFrame({"a": [0, 1], "b": ["a", "b"]})
basic_df = DataFrame[BasicSchema](df)
basic_df.validate()  # Ensures schema correctness
```

### Adding Custom Validations
Extend `DataFrame` and define validation methods prefixed with `check_`:

```python
class BasicSchemaDataFrame(DataFrame[BasicSchema]):
    def check_a_is_positive(self) -> Self:
        assert self.select(pl.col("a").ge(0).all()).item(), "Column a contains negative values!"
        return self

# Example usage
df = pl.DataFrame({"a": [0, 1]})
basic_df = BasicSchemaDataFrame(df)
basic_df.validate()  # Passes validation

# This will raise an AssertionError
df_invalid = pl.DataFrame({"a": [-1, 1]})
BasicSchemaDataFrame(df_invalid).validate()
```

### Get typing goodies !
Ensure row retrieval maintains proper types:

```python
row = basic_df.rows(named=True)[0]
# row is a typedDict !
```


