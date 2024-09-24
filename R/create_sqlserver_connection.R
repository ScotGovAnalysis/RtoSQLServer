#' Create a connection to a SQL Server database
#'
#' Establishes a connection to a SQL Server database
#' using the ODBC driver. This function uses Windows authentication only. Use
#' [`DBI::dbDisconnect()`] to disconnect this connection once no longer in use.
#'
#' @param server Server and instance where SQL Server database found.
#' @param database The name of the database to connect to.
#' @param timeout The timeout period (in seconds) for establishing
#' the connection. Defaults to 10.
#'
#' @return A connection object of class `"Microsoft SQL Server"`
#' from the `odbc` package.
#'
#' @examples
#' \dontrun{
#' # Connect to a SQL Server database
#' con <- create_sqlserver_connection(
#'   server = "my_server",
#'   database = "my_database"
#' )
#'
#' # Remember to disconnect after usage
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
create_sqlserver_connection <- function(server, database, timeout = 10) {
  tryCatch(
    {
      DBI::dbConnect(
        odbc::odbc(),
        Driver = "SQL Server",
        Trusted_Connection = "True",
        DATABASE = database,
        SERVER = server,
        timeout = timeout
      )
    },
    error = function(cond) {
      stop(glue::glue(
        "Failed to create connection to database: \\
        {database} on server: {server} \\
        \n{cond}"
      ), call. = FALSE)
    }
  )
}
