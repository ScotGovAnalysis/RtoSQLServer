drop_table_from_db <- function(database, server, schema, table_name) {
  sql <- paste0("DROP TABLE [", schema, "].[", table_name, "]")
  execute_sql(database = database, server = server, sql = sql, output = TRUE)
  message("Table: '", table_name, "' successfully deleted from database: '", database, "' on server '", server, "'")
}
