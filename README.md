
<!-- README.md is generated from README.Rmd. Please edit that file -->

# RtoSQLServer

<!-- badges: start -->

[![GitHub release (latest by
date)](https://img.shields.io/github/v/release/ScotGovAnalysis/RtoSQLServer)](https://github.com/ScotGovAnalysis/RtoSQLServer/releases/latest)
[![R-CMD-check](https://github.com/DataScienceScotland/RtoSQLServer/workflows/R-CMD-check/badge.svg)](https://github.com/DataScienceScotland/RtoSQLServer/actions)
<!-- badges: end -->

Load R data frames into an MS SQL Server database and modify MS SQL
Server tables with R. For help documentation, see [the package
website](https://scotgovanalysis.github.io/RtoSQLServer).

## Installation

If you are working within the Scottish Government, the package can be
installed in the same way as other R packages internally.

The R package can be installed directly from Github or locally from
zip.  
To install directly from GitHub:

``` r
remotes::install_github("DataScienceScotland/rtosqlserver", upgrade = "never")
```

If the above does not work, install by downloading:

1.  Download the [zip of the
    repository](https://github.com/DataScienceScotland/RtoSQLServer/archive/refs/heads/main.zip)
    from GitHub.
2.  Save the downloaded zip to a specific directory (e.g. C:/temp).
3.  Install with this command specifying the path to the downloaded zip:

``` r
remotes::install_local("C:/temp/RtoSQLServer-main.zip", upgrade="never")
```

## Functionality

As well as loading R dataframes into SQL Server databases, functions are
currently available to:

- [Read](https://scotgovanalysis.github.io/RtoSQLServer/reference/read_table_from_db.html)
  a database table into an R dataframe, optionally specifying a subset
  of table columns or row filter.
- [Rename](https://scotgovanalysis.github.io/RtoSQLServer/reference/rename_table.html)
  and
  [drop](https://scotgovanalysis.github.io/RtoSQLServer/reference/drop_table_from_db.html)
  a table from the database.
- [Add](https://scotgovanalysis.github.io/RtoSQLServer/reference/add_column.html),
  [drop](https://scotgovanalysis.github.io/RtoSQLServer/reference/drop_column.html),
  [rename](https://scotgovanalysis.github.io/RtoSQLServer/reference/rename_column.html)
  columns from existing database tables.
- Read existing [table
  metadata](https://scotgovanalysis.github.io/RtoSQLServer/reference/db_table_metadata.html)
  (columns, data types, summary info).
- [List all
  tables](https://scotgovanalysis.github.io/RtoSQLServer/reference/show_schema_tables.html)
  and views in a schema.
- Create an MS SQL Server [database connection
  object](https://scotgovanalysis.github.io/RtoSQLServer/reference/create_sqlserver_connection.html)
  for use with DBI or dbplyr packages.
- Run any other input [sql in the
  database](https://scotgovanalysis.github.io/RtoSQLServer/reference/execute_sql.html)
  and return a data frame if a select statement.

## Example

See [Get started](articles/RtoSQLServer.html) for examples of the main
functionality.

``` r
# Loading data frame example
library(RtoSQLServer)

# Make a test dataframe with n rows
test_n_rows <- 123456
test_df <- data.frame(a = rep("a", test_n_rows), b = rep("b", test_n_rows))

# Set database connection details:
server <- "server\\instance"
database <- "my_database_name"
schema <- "my_schema_name"

# Write the test dataframe to a SQL Server table in 100K batches (by default system versioning is FALSE)
write_dataframe_to_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_r_tbl",
  dataframe = test_df
)
```
