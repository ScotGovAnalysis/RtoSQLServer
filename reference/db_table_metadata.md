# Return metadata about an existing database table

Returns a dataframe of information about an existing table. This
includes the name of each column, its datatype and (optionally) its
range of values.

## Usage

``` r
db_table_metadata(server, database, schema, table_name, summary_stats = FALSE)
```

## Arguments

- server:

  Server-instance where SQL Server database running.

- database:

  Name of SQL Server database where table is found.

- schema:

  Name of schema in SQL Server database where table is found.

- table_name:

  Name of the table.

- summary_stats:

  Add summary stats of each col to metadata output. This includes
  ranges, number of distinct and number of NULL values. Defaults TRUE,
  however much query time is much quicker if FALSE as just returns col
  names and types.

## Value

Dataframe of table / column metadata.

## Examples

``` r
if (FALSE) { # \dontrun{
db_table_metadata(
  database = "my_database",
  server = "my_server",
  table_name = "my_table",
)
} # }
```
