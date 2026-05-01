# Delete rows from an existing database table

Can either delete all rows from the input table or just a subset by
specifying a filter. If using a filter, recommended to test it first
using it as a filter in
[`read_table_from_db()`](https://scotgovanalysis.github.io/RtoSQLServer/reference/read_table_from_db.md).
This ensures your filter is working and not deleting rows unexpectedly.

## Usage

``` r
delete_table_rows(server, database, schema, table_name, filter_stmt = NULL)
```

## Arguments

- server:

  Server instance where SQL Server database running.

- database:

  Database containing table with rows to delete.

- schema:

  Name of database schema containing table.

- table_name:

  Name of table with rows to delete.

- filter_stmt:

  Optional filter statement to delete only a subset of table rows where
  the filter is TRUE.

  - this should be a character expression in the format of a
    [`dplyr::filter()`](https://dplyr.tidyverse.org/reference/filter.html)
    query, for example `"Species == 'virginica'"` and it will be
    translated to SQL using
    [`dbplyr::translate_sql()`](https://dbplyr.tidyverse.org/reference/translate_sql.html).
    One way to achieve the right syntax for this argument is to pass a
    [`dplyr::filter()`](https://dplyr.tidyverse.org/reference/filter.html)
    expression through `deparse1(substitute())`, for example:
    `deparse1(substitute(Species == "virginica"))`.

## Examples

``` r
if (FALSE) { # \dontrun{
delete_table_rows(
  database = database,
  server = server,
  schema = schema,
  table_name = "test_iris",
  filter_stmt = "Species == 'setosa'"
)
} # }
```
