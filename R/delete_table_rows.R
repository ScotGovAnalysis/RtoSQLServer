delete_sql <- function(schema, table_name) {
  glue::glue_sql("DELETE FROM {`quoted_schema_tbl(schema, table_name)`}",
    .con = DBI::ANSI()
  )
}

truncate_sql <- function(schema, table_name) {
  glue::glue_sql("TRUNCATE TABLE {`quoted_schema_tbl(schema, table_name)`};",
    .con = DBI::ANSI()
  )
}

add_filter_sql <- function(initial_sql, filter_stmt) {
  filter_sql <- format_filter(filter_stmt)
  glue::glue(
    initial_sql,
    "WHERE {filter_sql};",
    .sep = " "
  )
}


#' Delete rows from an existing database table
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
#' @param filter_stmt Optional filter statement to delete only a subset
#' of table rows where the filter is TRUE.
#'  - this should be a character
#' expression in the format of a [dplyr::filter()] query,
#' for example `"Species == 'virginica'"` and it will be translated to SQL
#' using [dbplyr::translate_sql()]. One way to achieve the right
#' syntax for this argument is to pass a [dplyr::filter()] expression
#' through `deparse1(substitute())`, for example:
#' `deparse1(substitute(Species == "virginica"))`.
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
delete_table_rows <- function(server,
                              database,
                              schema,
                              table_name,
                              filter_stmt = NULL) {
  initial_row_count <- db_table_metadata(
    server,
    database,
    schema,
    table_name,
    TRUE
  )$row_count[1]
  if (is.null(filter_stmt)) {
    # Cannot use truncate on system versioned tables
    if (is_versioned(server, database, schema, table_name)) {
      sql <- delete_sql(schema, table_name)
    } else {
      sql <- truncate_sql(schema, table_name)
    }
  } else {
    initial_sql <- delete_sql(schema, table_name)
    sql <- add_filter_sql(initial_sql, filter_stmt)
  }

  message(glue::glue("Delete SQL Statement:
                     {sql}"))

  execute_sql(server, database, sql, output = FALSE)

  end_row_count <- db_table_metadata(
    server,
    database,
    schema,
    table_name,
    TRUE
  )$row_count[1]

  deleted_count <- initial_row_count - end_row_count

  message(glue::glue("Deleted {deleted_count} rows \\
                     from {schema}.{table_name}. \\
                     Table now has {end_row_count} rows."))
}
