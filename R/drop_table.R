#' Drop SQL Server table from database. Check if versioned table and disable versioning and drop history table too if so.
#'
#' @param database Database containing the table to be dropped.
#' @param server Server and instance where SQL Server database found.
#' @param schema Name of schema containing table to be dropped.
#' @param table_name Name of the table to be dropped.
#'
#' @export
#'
#' @examples
#' drop__table_from_db(database = "my_database", server = "my_server", schema = "my_schema", table_name = "table_to_drop")
drop_table_from_db <- function(database, server, schema, table_name) {
  check_sql <- paste0("select name, temporal_type, temporal_type_desc from sys.tables where name = '", table_name, "'")
  check_df <- execute_sql(database = database, server = server, sql = check_sql, output = TRUE)
  if (check_df[["temporal_type_desc"]] == "SYSTEM_VERSIONED_TEMPORAL_TABLE") {
    drop_sql <- paste0(
      "ALTER TABLE [", schema, "].[", table_name, "] SET ( SYSTEM_VERSIONING = OFF );",
      "DROP TABLE [", schema, "].[", table_name, "];",
      "DROP TABLE [", schema, "].[", table_name, "History]"
    )
  }
  else {
    drop_sql <- paste0("DROP TABLE [", schema, "].[", table_name, "]")
  }
  execute_sql(database = database, server = server, sql = drop_sql, output = TRUE)
  message("Table: '", table_name, "' successfully deleted from database: '", database, "' on server '", server, "'")
}
