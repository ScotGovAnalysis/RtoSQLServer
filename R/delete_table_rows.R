delete_sql <- function(schema, table_name) {
  glue::glue_sql("DELETE FROM {`schema`}.{`table_name`}", .con = DBI::ANSI())
}

truncate_sql <- function(schema, table_name) {
  glue::glue_sql("TRUNCATE TABLE {`schema`}.{`table_name`}", .con = DBI::ANSI())
}

add_filter_sql <- function(connection, initial_sql, filter_stmt) {
  filter_sql <- format_filter(connection, filter_stmt)
  glue::glue(
    initial_sql,
    "WHERE {filter_sql};",
    .sep = " "
  )
}

delete_table_rows <- function(database,
                              server,
                              schema,
                              table_name,
                              filter_stmt = NULL) {
  if (is.null(filter_stmt)) {
    sql <- truncate_sql(schema, table_name)
  } else {
    connection <- create_sqlserver_connection(
      server = server,
      database = database
    )
    initial_sql <- delete_sql(schema, table_name)
    sql <- add_filter_sql(connection, initial_sql, filter_stmt)
  }
  execute_sql(server, database, sql, output = FALSE)
}
