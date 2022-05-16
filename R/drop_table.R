#' Drop versioned SQL Server table from database
#'
#' @param database
#' @param server
#' @param schema
#' @param table_name
#'
#' @export
#'
#' @examples
#' drop_versioned_table_from_db(database = "my_database", server = "my_server", schema = "my_schema", table_name = "table_to_drop")
drop_versioned_table_from_db <- function(database, server, schema, table_name) {
  drop_sql <- list(
    paste0(
      "ALTER TABLE [", schema, "].[", table_name, "] SET ( SYSTEM_VERSIONING = OFF );",
      "DROP TABLE [", schema, "].[", table_name, "];",
      "DROP TABLE [", schema, "].[", table_name, "History]"
    )
  )
  for (sql in drop_sql) {
    execute_sql(database = database, server = server, sql = sql, output = TRUE)
  }
  message("Table: '", table_name, "' successfully deleted from database: '", database, "' on server '", server, "'")
}
