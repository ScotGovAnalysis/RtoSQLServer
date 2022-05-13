create_staging_table <- function(database, server, schema, table, dataframe) {
  columns <- sapply(dataframe, class)
  sql <- paste0("CREATE TABLE [", schema, "].[", table, "_staging_] (", table, "ID INT NOT NULL IDENTITY PRIMARY KEY,")
  for (column_index in seq_len(ncol(dataframe))) {
    column_name <- names(columns[column_index])
    data_type <- r_to_sql_data_type(columns[[column_index]][1])
    sql <- paste0(sql, " [", column_name, "] ", data_type, ", ")
  }
  sql <- paste0(substr(sql, 1, nchar(sql) - 2), ");")
  execute_sql(database = database, server = server, sql = sql, output = FALSE)
}



populate_staging_table <- function(database, server, schema, table, dataframe, overwrite = TRUE, append = FALSE) {
  connection <- create_sqlserver_connection(database = database, server = server)
  tables <- get_db_tables(database = database, server = server)
  if (nrow(tables[tables$Schema == schema & tables$Name == paste0(table, "_staging_"), ]) == 0) {
    create_staging_table(database = database, server = server, schema = schema, table = table, dataframe = dataframe)
    overwrite <- TRUE
  }
  tryCatch({
    DBI::dbWriteTable(connection, name = DBI::Id(schema = schema, table = paste0(table, "_staging_")), value = dataframe, overwrite = overwrite, append = append)
    message(paste0("Staging successfully written to database"))
  }, error = function(cond) {
    stop(paste0("Failed to write staging data to database.\nOriginal error message: ", cond))
  })
  DBI::dbDisconnect(connection)
}



populate_table_from_staging <- function(database, server, schema, table) {
  metadata <- db_table_metadata(database = database, server = server, schema = schema, table = paste0(table, "_staging_"))
  column_string <- ""
  for (row in seq_len(nrow(metadata))) {
    column_name <- metadata[row, 1]
    column_string <- paste0(column_string, " [", column_name, "], ")
  }
  column_string <- substr(column_string, 1, nchar(column_string) - 2)
  sql <- paste0("DELETE FROM [", schema, "].[", table, "];
                INSERT INTO [", schema, "].[", table, "] (", column_string, ") select ", column_string, " from [", schema, "].[", table, "_staging_];")
  execute_sql(database = database, server = server, sql = sql, output = FALSE)
}



delete_staging_table <- function(database, server, schema, table) {
  connection <- create_sqlserver_connection(database = database, server = server)
  tryCatch({
    odbc::dbRemoveTable(conn = connection, DBI::Id(schema = schema, table = paste0(table, "_staging_")))
  }, error = function(cond) {
    stop(paste0("Failed to delete staging table: '", table, "' from database: '", database, "' on server: '", server, "'\nOriginal error message: ", cond))
  })
  DBI::dbDisconnect(connection)
  message("Staging table: '", table, "' successfully deleted from database: '", database, "' on server '", server, "'")
}



write_dataframe_to_db <- function(database, server, schema, table_name, dataframe, versioned=TRUE) {
  populate_staging_table(database = database, server = server, schema = schema, table = table_name, dataframe = dataframe)
  connection <- create_sqlserver_connection(database = database, server = server)
  tables <- get_db_tables(database = database, server = server)
  if (nrow(tables[tables$Schema == schema & tables$Name == table_name, ]) == 0) {
    if (versioned){
    create_versioned_table(database = database, server = server, schema = schema, table = table_name)}
    else {create_table(database = database, server = server, schema = schema, table = table_name)}
  }
  tryCatch({
    populate_table_from_staging(database = database, server = server, schema = schema, table = table_name)
    message(paste0("Dataframe successfully written to: '", table_name, "'"))
  }, error = function(cond) {
    stop(paste0("Failed to write dataframe to database: '", database, "'\nOriginal error message: ", cond))
  })
  delete_staging_table(database = database, server = server, schema = schema, table = table_name)
  DBI::dbDisconnect(connection)
}