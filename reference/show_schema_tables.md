# Show all tables (and optionally views) in a schema

Returns a data frame of table name, creation date and optionally whether
table or view. Queries MS SQL Server sys.tables or sys.objects to
extract this information.

## Usage

``` r
show_schema_tables(server, database, schema, include_views = FALSE)
```

## Arguments

- server:

  Server instance where SQL Server database running.

- database:

  Database containing schema to list tables from.

- schema:

  Name of database schema to list tables from.

- include_views:

  If TRUE includes views as well as tables and adds an object_type
  column to indicate which each is. Defaults to FALSE.

## Value

Dataframe of table names, creation dates and optionally whether table or
view.

## Examples

``` r
if (FALSE) { # \dontrun{
show_schema_tables(
  server = "my_server",
  database = "my_database",
  schema = "my_schema",
  include_views = TRUE
)
} # }
```
