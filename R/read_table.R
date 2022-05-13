read_table_from_db <- function(database, server, schema, table_name, columns = NULL) {
  id_column <- paste0(table_name, "ID")
  select_list <- table_select_list(columns)
  sql <- paste0("SELECT ", select_list,
                " FROM [", schema, "].[", table_name, "];")
  execute_sql(database = database, server = server, sql = sql, output = TRUE)
}
