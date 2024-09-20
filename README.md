
<!-- README.md is generated from README.Rmd. Please edit that file -->

# RtoSQLServer

<!-- badges: start -->

[![GitHub release (latest by
date)](https://img.shields.io/github/v/release/ScotGovAnalysis/sgplot)](https://github.com/ScotGovAnalysis/sgplot/releases/latest)
[![R-CMD-check](https://github.com/DataScienceScotland/RtoSQLServer/workflows/R-CMD-check/badge.svg)](https://github.com/DataScienceScotland/RtoSQLServer/actions)
<!-- badges: end -->

Load R dataframes into a MS SQL Server database. Manage MS SQL Server
tables with R.

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
also currently available to:

- Read a database table into an R dataframe, optionally specifying a
  subset of table columns or row filter.
- Rename and drop a table from the database.
- Add, drop, rename columns from existing database tables.
- Read existing table metadata (columns, data types, summary info).
- List all tables and views in a schema.
- Create an MS SQL Server database connection object for use with DBI or
  dbplyr packages.
- Run any other input sql in the database and return a dataframe if a
  select statement.

## Example Usage

Here is an example using the main functions:

``` r
# Make a test dataframe with n rows
test_n_rows <- 1234567
test_df <- data.frame(a = rep("a", test_n_rows), b = rep("b", test_n_rows))

# Set database connection details for use in functions:
server <- "server\\instance"
database <- "my_database_name"
schema <- "my_schema_name"

# Write the test dataframe to a SQL Server table in 100K batches (by default system versioning is FALSE)
write_dataframe_to_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_r_tbl",
  dataframe = test_df,
  append_to_existing = FALSE,
  batch_size = 1e5,
  versioned_table = FALSE
)

# Read the SQL Server table into an R dataframe
read_df <- read_table_from_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_r_tbl"
)

# To return the names and creation date of all
# existing tables and views in a schema
# into an R dataframe

schema_objects_df <- show_schema_tables(
  server = server,
  database = database,
  schema = schema,
  include_views = TRUE
)

# To return information about existing database table
# column names, datatype and optionally summary info
# use db_table_metadata

db_table_metadata(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_r_tbl",
  summary_stats = FALSE
)

# (use summary_stats = TRUE if want to know value ranges,
# distinct counts, number of NULL records,
# but this is slower)

add_column(server = server,
           database = database,
           schema = schema,
           table_name = "test_r_tbl",
           column_name = "my_new_col",
           )


# Drop a table from the database
drop_table_from_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_r_tbl"
)
```
