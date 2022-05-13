drop_versioned_table_from_db <- function(database, server, schema, table_name) {
  drop_sql <- list(
    paste0("ALTER TABLE [", schema, "].[", table_name, "] SET ( SYSTEM_VERSIONING = OFF )"),
    paste0("DROP TABLE [", schema, "].[", table_name, "]"),
    paste0("DROP TABLE [", schema, "].[", table_name, "History]")
  )
  for (sql in drop_sql) {
    execute_sql(database = database, server = server, sql = sql, output = TRUE)
  }
  message("Table: '", table_name, "' successfully deleted from database: '", database, "' on server '", server, "'")
}
