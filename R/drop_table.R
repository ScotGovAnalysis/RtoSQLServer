#' Drop versioned SQL Server table from database.
#'
#' @param database Database containing the table to be dropped.
#' @param server Server and instance where SQL Server database found.
#' @param schema Name of schema containing table to be dropped.
#' @param table_name Name of the table to be dropped.
#'
#' @export
#'
#' @examples
#' drop_versioned_table_from_db(database = "my_database", server = "my_server", schema = "my_schema", table_name = "table_to_drop")
drop_versioned_table_from_db <- function(database, server, schema, table_name) {
  drop_sql <- paste0(
    "ALTER TABLE [", schema, "].[", table_name, "] SET ( SYSTEM_VERSIONING = OFF );",
    "DROP TABLE [", schema, "].[", table_name, "];",
    "DROP TABLE [", schema, "].[", table_name, "History]"
  )
  execute_sql(database = database, server = server, sql = drop_sql, output = TRUE)
  message("Table: '", table_name, "' successfully deleted from database: '", database, "' on server '", server, "'")
}
