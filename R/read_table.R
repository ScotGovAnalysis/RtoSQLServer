table_select_list <- function(server,
                              database,
                              schema,
                              table_name,
                              columns,
                              include_pk) {
  # all column names found in existing table
  existing_cols <- db_table_metadata(
    server,
    database,
    schema,
    table_name
  )$column_name

  # Get the primary key column name
  pk <- get_pk_name(server, database, schema, table_name)

  # if user specified column list - add pk to it if they've set to TRUE
  if (!is.null(columns)) {
    if (include_pk) {
      if (!pk %in% columns) {
        columns <- c(pk, columns)
      }
    }
    # check the columns all exist, if not fail
    not_exist_cols <- setdiff(columns, existing_cols)
    if (length(not_exist_cols) > 0) {
      stop(glue::glue(
        "Column {not_exist_cols} not found in {schema}.{table_name}."
      ))
    } else {
      return(columns)
    }

    # where no user specified list of columns
  } else {
    if (!include_pk) {
      return(existing_cols[existing_cols != pk])
    } else {
      return(existing_cols)
    }
  }
}


format_filter <- function(connection, filter_stmt) {
  sql <- dbplyr::translate_sql(!!rlang::parse_expr(filter_stmt),
    con = connection
  )
  sql <- as.character(sql)
}

create_read_sql <- function(connection,
                            schema,
                            select_list,
                            table_name,
                            filter_stmt) {
  initial_sql <- glue::glue_sql(
    "SELECT {`select_list`*} FROM {`quoted_schema_tbl(schema, table_name)`}",
    .con = connection
  )
  if (!is.null(filter_stmt)) {
    filter_stmt <- format_filter(connection, filter_stmt)
    glue::glue(
      initial_sql,
      "WHERE {filter_stmt};",
      .sep = " "
    )
  } else {
    glue::glue(initial_sql, ";")
  }
}


#' Read a SQL Server table into an R dataframe.
#'
#' If you are confident in writing SQL you may prefer to
#' use the [RtoSQLServer::execute_sql()] function instead.
#'
#'
#' @param server Server instance where SQL Server database running.
#' @param database Database containing table to read.
#' @param schema Name of database schema containing table to read.
#' @param table_name Name of table in database to read.
#' @param columns Optional vector of column names to select.
#' @param filter_stmt Optional filter statement - this should be a character
#' @param include_pk Whether to include primary key column in output dataframe.
#' The primary key is added automatically when a table is loaded into the
#' database as <table_name>ID. Defaults to FALSE.
#' expression in the format of a [dplyr::filter()] query,
#' for example `"Species == 'virginica'"` and it will be translated to SQL
#' using [dbplyr::translate_sql()].
#'
#' @return Dataframe of table.
#' @export
#'
#' @examples
#' \dontrun{
#' read_table_from_db(
#'   database = "my_database",
#'   server = "my_server",
#'   table_name = "my_table",
#'   columns = c("column1", "column2"),
#'   filter_stmt = "column1 < 5 & column2 == 'b'"
#' )
#' }
read_table_from_db <- function(database,
                               server,
                               schema,
                               table_name,
                               columns = NULL,
                               filter_stmt = NULL,
                               include_pk = FALSE) {
  if (!check_table_exists(
    server,
    database,
    schema,
    table_name
  )) {
    stop(glue::glue(
      "Table: {schema}.{table_name} does not exist in the database."
    ))
  }
  connection <- create_sqlserver_connection(
    server = server,
    database = database
  )

  select_list <- table_select_list(
    server,
    database,
    schema,
    table_name,
    columns,
    include_pk
  )

  read_sql <- create_read_sql(
    connection,
    schema,
    select_list,
    table_name,
    filter_stmt
  )
  DBI::dbDisconnect(connection)
  message(glue::glue("Read SQL statement:\n{read_sql}"))
  execute_sql(
    database = database, server =
      server, sql = read_sql, output = TRUE
  )
}
