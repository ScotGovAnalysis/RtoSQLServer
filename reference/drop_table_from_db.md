# Drop SQL Server table from database

Drop specified table. Check if versioned table. If so attempt to disable
versioning and drop history table too if so. Extra permissions may be
required to drop a versioned table so contact system admin if receive an
error showing this is the case.

## Usage

``` r
drop_table_from_db(
  server,
  database,
  schema,
  table_name,
  versioned_table = FALSE,
  silent = FALSE
)
```

## Arguments

- server:

  Server and instance where SQL Server database found.

- database:

  Database containing the table to be dropped.

- schema:

  Name of schema containing table to be dropped.

- table_name:

  Name of the table to be dropped.

- versioned_table:

  Is this a versioned table. Legacy argument no longer used. This is now
  checked every time regardless of T or F input.

- silent:

  If TRUE do not give message that dropping complete. Defaults to FALSE.

## Examples

``` r
if (FALSE) { # \dontrun{
drop_table_from_db(
  database = "my_database",
  server = "my_server",
  schema = "my_schema",
  table_name = "table_to_drop"
)
} # }
```
