#' Read a SQL Server table into an R dataframe,
#'
#' @param database Database containing table to read.
#' @param server Server instance where SQL Server database running.
#' @param schema Name of database schema containing table to read.
#' @param table_name Name of table in database to read.
#' @param columns Optional vector of column names to select.
#'
#' @return Dataframe of table.
#' @export
#'
#' @examples
#' read_table_from_db(database = "my_database", server = "my_server", table_name = "my_table", columns = c("column1", "column2"))
read_table_from_db <- function(database, server, schema, table_name, columns = NULL) {
  id_column <- paste0(table_name, "ID")
  select_list <- table_select_list(columns)
  sql <- paste0(
    "SELECT ", select_list,
    " FROM [", schema, "].[", table_name, "];"
  )
  execute_sql(database = database, server = server, sql = sql, output = TRUE)
}
