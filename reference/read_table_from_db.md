# Read a SQL Server table into an R dataframe

The SQL Server table (or view) can have column or row filters applied
before it is read into an R data frame. This can help if reading from a
large table and not all the data is required in your R session. See
[`vignette("read_filter")`](https://scotgovanalysis.github.io/RtoSQLServer/articles/read_filter.md)
for more information.

## Usage

``` r
read_table_from_db(
  server,
  database,
  schema,
  table_name,
  columns = NULL,
  filter_stmt = NULL,
  include_pk = FALSE,
  cast_datetime2 = TRUE
)
```

## Arguments

- server:

  Server instance where SQL Server database running.

- database:

  Database containing table to read.

- schema:

  Name of database schema containing table to read.

- table_name:

  Name of table in database to read.

- columns:

  Optional vector of column names to select.

- filter_stmt:

  Optional filter statement to only read a subset of rows from the
  specified database table.

  - this should be a character expression in the format of a
    [`dplyr::filter()`](https://dplyr.tidyverse.org/reference/filter.html)
    query, for example `"Species == 'virginica'"` and it will be
    translated to SQL using
    [`dbplyr::translate_sql()`](https://dbplyr.tidyverse.org/reference/translate_sql.html).
    One way to achieve the right syntax for this argument is to pass a
    [`dplyr::filter()`](https://dplyr.tidyverse.org/reference/filter.html)
    expression through `deparse1(substitute())`, for example
    `deparse1(substitute(Species == "virginica"))`

- include_pk:

  Whether to include primary key column in output dataframe. A primary
  key column is added automatically when a table is loaded into the
  database using `create_replace_table()` as \<table_name\>ID. Defaults
  to FALSE.

- cast_datetime2:

  Cast `datetime2` data type columns to `datetime`. This is to help
  older ODBC drivers where datetime2 columns are read into R as
  character when should be POSIXct. Defaults to TRUE.

## Value

Dataframe of table.

## Details

If you are confident in writing SQL you may prefer to use the
[`execute_sql()`](https://scotgovanalysis.github.io/RtoSQLServer/reference/execute_sql.md)
function instead.

## Examples

``` r
if (FALSE) { # \dontrun{
read_table_from_db(
  server = "my_server",
  database = "my_database",
  schema = "my_schema",
  table_name = "my_table",
  columns = c("column1", "column2"),
  filter_stmt = "column1 < 5 & column2 == 'b'"
)
} # }
```
