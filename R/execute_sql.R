#' Connect, execute SQL in SQL Server database and then disconnect from
#' database.
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
#' sql_to_run <- "select test_column, other_column from my_test_table
#' where other_column > 10"
#' execute_sql(
#'   database = my_database, server = my_server,
#'   sql = sql_to_run, output = TRUE
#' )
execute_sql <- function(server, database, sql, output = FALSE) {
  connection <- create_sqlserver_connection(
    server = server,
    database = database
  )
  for (i in seq_len(length(sql))) {
    if (output) {
      tryCatch(
        {
          output_data <- DBI::dbGetQuery(connection, sql[i])
        },
        error = function(cond) {
          stop(paste0("Failed to execute SQL.\nOriginal error message: ", cond))
        }
      )
    } else {
      tryCatch(
        {
          DBI::dbExecute(connection, sql[i])
          output_data <- paste0(sql, " executed successfully")
        },
        error = function(cond) {
          stop(paste0("Failed to execute SQL.\nOriginal error message: ", cond))
        }
      )
    }
  }
  DBI::dbDisconnect(connection)
  return(output_data)
}
