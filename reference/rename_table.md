# Rename a table in the database

Renames a specified table. The function checks if the table exists in
the schema before attempting to rename it. In MS SQL Server, this is
done using `EXEC sp_rename`.

## Usage

``` r
rename_table(server, database, schema, old_table_name, new_table_name)
```

## Arguments

- server:

  Server and instance where SQL Server database found.

- database:

  Database containing the table to be renamed.

- schema:

  Name of the schema containing the table.

- old_table_name:

  The current name of the table to be renamed.

- new_table_name:

  The new name for the table.

## Examples

``` r
if (FALSE) { # \dontrun{
# Rename test_iris table to test_iris_renamed
rename_table(
  server = "my_server",
  database = "my_database",
  schema = "my_schema",
  old_table_name = "test_iris",
  new_table_name = "test_iris_renamed"
)
} # }
```
