# Write an R data frame to a SQL Server table

R data frame rows are initially loaded into a staging table in batches.
See
[`vignette("load_method")`](https://scotgovanalysis.github.io/RtoSQLServer/articles/load_method.md)
for more information. The data frame column data types are mapped to
equivalents in SQL Server.

## Usage

``` r
write_dataframe_to_db(
  server,
  database,
  schema,
  table_name,
  dataframe,
  append_to_existing = FALSE,
  batch_size = 1e+05,
  versioned_table = FALSE
)
```

## Arguments

- server:

  Server instance where SQL Server database running.

- database:

  Name of SQL Server database where table will be written.

- schema:

  Name of schema in SQL Server database where table will be created.

- table_name:

  Name of table to be created in SQL Server database.

- dataframe:

  Source R dataframe that will be written to SQL Server database.

- append_to_existing:

  Boolean if TRUE then rows will be appended to existing database table
  (if exists). Default FALSE.

- batch_size:

  Source R dataframe rows will be loaded into a staging SQL Server table
  in batches of this many rows at a time.

- versioned_table:

  Create table with SQL Server system versioning. Defaults to FALSE. If
  appending to existing table will not change existing versioning
  status. If overwriting an existing versioned table may receive
  permissions error due to dropping existing versioned table failing.
  Contact a sysadmin to drop the versioned table for you.

## Details

Can create table optionally with system versioning on. However, extra
permissions may be required to drop or overwrite system versioned tables
so use this option with caution.

## Examples

``` r
if (FALSE) { # \dontrun{
write_dataframe_to_db(
  server = "my_server",
  schema = "my_schema",
  table_name = "output_table",
  dataframe = my_df
)
} # }
```
