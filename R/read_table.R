table_select_list <- function(columns) {
  if (is.null(columns)) {
    "*"
  } else {
    glue::glue_collapse(glue::glue("[{columns}]"), sep = ", ")
  }
}

format_filter <- function(server, database, filter_stmt) {
  connection <- create_sqlserver_connection(
    server = server,
    database = database
  )
  sql <- dbplyr::translate_sql(!!rlang::parse_expr(filter_stmt),
    con = connection
  )
  DBI::dbDisconnect(connection)
  gsub("(`|\")([^=]*)\\1", "[\\2]", sql)
}

create_read_sql <- function(server,
                            database,
                            schema,
                            select_list,
                            table_name,
                            filter_stmt) {
  initial_sql <- glue::glue(
    "SELECT {select_list}",
    "FROM [{schema}].[{table_name}]",
    .sep = " "
  )
  if (!is.null(filter_stmt)) {
    filter_stmt <- format_filter(server, database, filter_stmt)
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
                               filter_stmt = NULL) {
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
  select_list <- table_select_list(columns)

  read_sql <- create_read_sql(
    server,
    database,
    schema,
    select_list,
    table_name,
    filter_stmt
  )
  message(glue::glue("Read SQL statement:\n{read_sql}"))
  execute_sql(
    database = database, server =
      server, sql = read_sql, output = TRUE
  )
}
