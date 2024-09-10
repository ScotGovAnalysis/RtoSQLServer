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


#' Delete rows from an existing database table.
#'
#' Can either delete all rows from the input table or just a subset
#' by specifying a filter. If using a filter, recommended to test it
#' first using it as a filter in [`read_table_from_db()`]. This ensures
#' your filter is working and not deleting rows unexpectedly.
#'
#'
#' @param server Server instance where SQL Server database running.
#' @param database Database containing table with rows to delete.
#' @param schema Name of database schema containing table.
#' @param table_name Name of table with rows to delete.
#' @param filter_stmt Optional filter statement to delete a subset of
#' rows from the specified database table.
#'  - this should be a character
#' expression in the format of a [dplyr::filter()] query,
#' for example `"Species == 'virginica'"` and it will be translated to SQL
#' using [dbplyr::translate_sql()]. One way to achieve the right
#' syntax for this argument is to pass a [dplyr::filter()] expression
#' through `deparse1(substitute())`, for example
#' `deparse1(substitute(Species == "virginica"))`
#' @param cast_datetime2 Cast `datetime2` data type columns to `datetime`.
#' This is to help older ODBC drivers where datetime2 columns are read into R
#' as character when should be POSIXct. Defaults to TRUE.
#' @export
#'
#' @examples
#' \dontrun{
#' delete_table_rows(
#'   database = database,
#'   server = server,
#'   schema = schema,
#'   table_name = "test_iris",
#'   filter_stmt = "Species == 'setosa'"
#' )
#' }
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
