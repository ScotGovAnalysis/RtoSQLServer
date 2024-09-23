#' Connect, execute SQL in SQL Server database and then disconnect
#'
#' @param server Server instance where SQL Server database running.
#' @param database SQL Server database in which SQL executed.
#' @param sql SQL to be executed in the database.
#' @param output If TRUE write output of SQL to Dataframe. Defaults to FALSE.
#'
#' @return Dataframe or NULL depending on SQL executed.
#' @export
#'
#' @examples
#' \dontrun{
#' sql_to_run <- "select test_column, other_column from my_test_table
#' where other_column > 10"
#' execute_sql(
#'   database = "my_database", server = "my_server",
#'   sql = sql_to_run, output = TRUE
#' )
#' }
execute_sql <- function(server, database, sql, output = FALSE) {
  connection <- create_sqlserver_connection(
    server = server,
    database = database
  )

  tryCatch(
    {
      if (output) {
        output_data <- DBI::dbGetQuery(connection, sql)
      } else {
        DBI::dbExecute(connection, sql)
        output_data <- glue::glue("SQL: {sql}\nexecuted successfully")
      }
    },
    error = function(cond) {
      stop(glue::glue(
        "Failed to execute SQL. ODBC error message:\n{cond$message}"
      ), call. = FALSE)
    },
    finally = {
      DBI::dbDisconnect(connection)
    }
  )

  return(output_data)
}
