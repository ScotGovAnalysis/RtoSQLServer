# Rename a column in an existing database table

Renames a specified column in a table. The function checks if the table
and the column exist in the schema before attempting to rename the
column. In MS SQL Server this is done using `EXEC sp_rename`.

## Usage

``` r
rename_column(
  server,
  database,
  schema,
  table_name,
  old_column_name,
  new_column_name
)
```

## Arguments

- server:

  Server and instance where SQL Server database found.

- database:

  Database containing the table in which the column belongs.

- schema:

  Name of the schema containing the table.

- table_name:

  Name of the table containing the column.

- old_column_name:

  The current name of the column to be renamed.

- new_column_name:

  The new name for the column.

## Examples

``` r
if (FALSE) { # \dontrun{
# Rename Species column to SpeciesRenamed in test_iris table
rename_column(
  server = "my_server",
  database = "my_database",
  schema = "my_schema",
  table_name = "test_iris",
  old_column_name = "Species",
  new_column_name = "SpeciesRenamed"
)
} # }
```
