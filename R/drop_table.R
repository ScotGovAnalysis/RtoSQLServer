#' Drop SQL Server table from database. Check if versioned table and disable
#' versioning and drop history table too if so.
#'
#' @param server Server and instance where SQL Server database found.
#' @param database Database containing the table to be dropped.
#' @param schema Name of schema containing table to be dropped.
#' @param table_name Name of the table to be dropped.
#' @param versioned_table Is this a versioned table. Defaults to FALSE.
#' @param silent If TRUE do not give message that dropping complete.
#' Defaults to FALSE.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' drop_table_from_db(
#'   database = "my_database",
#'   server = "my_server",
#'   schema = "my_schema",
#'   table_name = "table_to_drop"
#' )
#' }
drop_table_from_db <- function(server,
                               database,
                               schema,
                               table_name,
                               versioned_table = FALSE,
                               silent = FALSE) {
  tables <- get_db_tables(database = database, server = server)
  if (nrow(tables[tables$Schema == schema &
    tables$Name == table_name, ]) == 0) {
    stop(paste0(
      "Table '", schema, ".", table_name,
      "' does not exist in the database."
    ))
  }
  if (versioned_table) {
    check_sql <- paste0("select name, temporal_type,
                        temporal_type_desc
                        from sys.tables where name = '", table_name, "'")
    check_df <- execute_sql(
      server = server,
      database = database,
      sql = check_sql,
      output = TRUE
    )
    if (check_df[["temporal_type_desc"]] ==
      "SYSTEM_VERSIONED_TEMPORAL_TABLE") {
      drop_sql <- paste0(
        "ALTER TABLE [", schema, "].[", table_name, "]
        SET ( SYSTEM_VERSIONING = OFF );",
        "DROP TABLE [", schema, "].[", table_name, "];",
        "DROP TABLE [", schema, "].[", table_name, "History]"
      )
    }
    # In case use the versioned_table argument as TRUE but not versioned table
    else {
      drop_sql <- paste0("DROP TABLE
                         [", schema, "].[", table_name, "]")
    }
  }
  # If versioned_table argument is FALSE
  else {
    drop_sql <- paste0("DROP TABLE
                       [", schema, "].[", table_name, "]")
  }
  execute_sql(
    server = server,
    database = database,
    sql = drop_sql,
    output = TRUE
  )
  if (!silent) {
    message(
      "Table: '",
      schema, ".",
      table_name,
      "' successfully deleted from database: '",
      database, "' on server '",
      server, "'"
    )
  }
}
