# Connect, execute SQL in SQL Server database and then disconnect

Connect, execute SQL in SQL Server database and then disconnect

## Usage

``` r
execute_sql(server, database, sql, output = FALSE)
```

## Arguments

- server:

  Server instance where SQL Server database running.

- database:

  SQL Server database in which SQL executed.

- sql:

  SQL to be executed in the database.

- output:

  If TRUE write output of SQL to Dataframe. Defaults to FALSE.

## Value

Dataframe or NULL depending on SQL executed.

## Examples

``` r
if (FALSE) { # \dontrun{
sql_to_run <- "select test_column, other_column from my_test_table
where other_column > 10"
execute_sql(
  database = "my_database", server = "my_server",
  sql = sql_to_run, output = TRUE
)
} # }
```
