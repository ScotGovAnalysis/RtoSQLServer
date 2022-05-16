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
  tryCatch(
    {
      DBI::dbWriteTable(connection, name = DBI::Id(schema = schema, table = paste0(table, "_staging_")), value = dataframe, overwrite = overwrite, append = append)
      message(paste0("Staging successfully written to database"))
    },
    error = function(cond) {
      stop(paste0("Failed to write staging data to database.\nOriginal error message: ", cond))
    }
  )
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
  tryCatch(
    {
      odbc::dbRemoveTable(conn = connection, DBI::Id(schema = schema, table = paste0(table, "_staging_")))
    },
    error = function(cond) {
      stop(paste0("Failed to delete staging table: '", table, "' from database: '", database, "' on server: '", server, "'\nOriginal error message: ", cond))
    }
  )
  DBI::dbDisconnect(connection)
  message("Staging table: '", table, "' successfully deleted from database: '", database, "' on server '", server, "'")
}



create_versioned_table <- function(database, server, schema, table) {
  metadata <- db_table_metadata(database = database, server = server, schema = schema, table = paste0(table, "_staging_"))
  sql <- paste0("CREATE TABLE [", schema, "].[", table, "] (", table, "ID INT NOT NULL IDENTITY PRIMARY KEY,")
  for (row in seq_len(nrow(metadata))) {
    column_name <- metadata[row, "ColumnName"]
    data_type <- metadata[row, "DataType"]
    sql <- paste0(sql, " [", column_name, "] ", data_type, ", ")
  }
  sql <- paste0(
    sql,
    "SysStartTime DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL, ",
    "SysEndTime DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL, ",
    "PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)) ",
    "WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [", schema, "].[", table, "History]));"
  )
  connection <- create_sqlserver_connection(database = database, server = server)
  DBI::dbGetQuery(connection, sql)
  DBI::dbDisconnect(connection)
}



#' Write an R dataframe to SQL Server table with system versioning
#'
#' @param database Name of SQL Server database where table will be written.
#' @param server Server instance where SQL Server database running.
#' @param schema Name of schema in SQL Server database where table will be created.
#' @param table_name Name of table to be created in SQL Server database.
#' @param dataframe Source dataframe that will be written to SQL Server database.
#'
#'
#' @export
#'
#' @examples
#' write_dataframe_to_db(database = "my_database", server = "my_server", schema = "my_schema", table_name = "output_table", dataframe = my_df)
write_dataframe_to_db <- function(database, server, schema, table_name, dataframe) {
  populate_staging_table(database = database, server = server, schema = schema, table = table_name, dataframe = dataframe)
  connection <- create_sqlserver_connection(database = database, server = server)
  tables <- get_db_tables(database = database, server = server)
  if (nrow(tables[tables$Schema == schema & tables$Name == table_name, ]) == 0) {
    create_versioned_table(database = database, server = server, schema = schema, table = table_name)
  }
  tryCatch(
    {
      populate_table_from_staging(database = database, server = server, schema = schema, table = table_name)
      message(paste0("Dataframe successfully written to: '", table_name, "'"))
    },
    error = function(cond) {
      stop(paste0("Failed to write dataframe to database: '", database, "'\nOriginal error message: ", cond))
    }
  )
  delete_staging_table(database = database, server = server, schema = schema, table = table_name)
  DBI::dbDisconnect(connection)
}
