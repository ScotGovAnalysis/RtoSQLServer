# Add a Column to an existing database table

Adds a specified column to a table. The column data type can be mapped
from an R object, or else the database column data type must be
explicitly specified.

## Usage

``` r
add_column(
  server,
  database,
  schema,
  table_name,
  column_name,
  sql_data_type = NULL,
  sample_value = NULL
)
```

## Arguments

- server:

  Server and instance where SQL Server database found.

- database:

  Database containing the table to which the column will be added.

- schema:

  Name of schema containing the table.

- table_name:

  Name of the table to which the column should be added.

- column_name:

  The name of the column to be added.

- sql_data_type:

  The SQL datatype for the column as a character string. For example
  `"nvarchar(255)"` (optional, can be inferred from `sample_value`
  alternatively). Use
  [`db_table_metadata()`](https://scotgovanalysis.github.io/RtoSQLServer/reference/db_table_metadata.md)
  to see the data types for existing tables/columns in the database, or
  refer to MS SQL Server guidance on data types.

- sample_value:

  Existing R data frame column, or a value that defines the datatype of
  the column. The input should be of the correct R class for its type,
  for example, strings as character, numbers as `numeric`, dates as
  `Date`, date times as `POSIXct` or `POSIXlt`. Optional and superseded
  by `sql_data_type` if specified. Some example valid inputs:
  `iris$Species`, `"a sample string"`,
  [`numeric()`](https://rdrr.io/r/base/numeric.html),
  `as.Date("2024-01-01"`).

## Examples

``` r
if (FALSE) { # \dontrun{
# Add a Species_new column to test_iris table
add_column(
  server = "my_server",
  database = "my_database",
  schema = "my_schema",
  table_name = "test_iris",
  column_name = "Species_new",
  sample_value = character(0)
)
} # }
```
