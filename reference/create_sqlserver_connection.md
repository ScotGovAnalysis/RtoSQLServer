# Create a connection to a SQL Server database

Establishes a connection to a SQL Server database using the ODBC driver.
This function uses Windows authentication only. Use
[`DBI::dbDisconnect()`](https://dbi.r-dbi.org/reference/dbDisconnect.html)
to disconnect this connection once no longer in use.

## Usage

``` r
create_sqlserver_connection(server, database, timeout = 10)
```

## Arguments

- server:

  Server and instance where SQL Server database found.

- database:

  The name of the database to connect to.

- timeout:

  The timeout period (in seconds) for establishing the connection.
  Defaults to 10.

## Value

A connection object of class `"Microsoft SQL Server"` from the `odbc`
package.

## Examples

``` r
if (FALSE) { # \dontrun{
# Connect to a SQL Server database
con <- create_sqlserver_connection(
  server = "my_server",
  database = "my_database"
)

# Remember to disconnect after usage
DBI::dbDisconnect(con)
} # }
```
