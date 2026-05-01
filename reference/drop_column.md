# Drop a Column from an existing database table

Drops a specified column from a table. Checks if the table exists in the
schema and if the column is present in the table before attempting to
drop the column.

## Usage

``` r
drop_column(server, database, schema, table_name, column_name)
```

## Arguments

- server:

  Server and instance where SQL Server database found.

- database:

  Database containing the table with column to drop.

- schema:

  Name of schema containing the table.

- table_name:

  Name of the table from which the column should be dropped.

- column_name:

  The name of the column to be dropped.

## Examples

``` r
if (FALSE) { # \dontrun{
# Drop the Species column from test_iris table
drop_column(
  server = "my_server",
  database = "my_database",
  schema = "my_schema",
  table_name = "test_iris",
  column_name = "Species"
)
} # }
```
