table_sql <- function(schema) {
  glue::glue_sql("SELECT name AS 'table',
       create_date AS 'creation_date'
       FROM sys.tables
       WHERE SCHEMA_NAME(schema_id) = {schema}
       order by 2", .con = DBI::ANSI())
}

table_view_sql <- function(schema) {
  glue::glue_sql("SELECT name AS 'table',
  type_desc AS 'object_type',
  create_date AS 'creation_date'
  FROM sys.objects
  WHERE type IN ('U', 'V')  -- 'U'ser tables, 'V'iews
  AND SCHEMA_NAME(schema_id) = {schema}
  order by type, name", .con = DBI::ANSI())
}

#' Show all tables (and optionally views) in a schema.
#'
#' Returns a data frame of table name, creation date and optionally whether
#' table or view. Queries MS SQL Server sys.tables to extract this information.
#'
#' @param server Server instance where SQL Server database running.
#' @param database Database containing schema to list tables from.
#' @param schema Name of database schema to list tables from.
#' @param include_views If TRUE includes views as well as tables and adds
#' an object_type column to indicate which each is. Defaults to FALSE.
#' @return Dataframe of table names, creation dates and optionally whether
#' table or view.
#' @export
#'
#' @examples
#' \dontrun{
#' show_schema_tables(
#'   server = "my_server",
#'   database = "my_database",
#'   schema = "my_schema",
#'   include_views = TRUE
#' )
#' }
show_schema_tables <- function(server,
                               database,
                               schema,
                               include_views = FALSE) {
  if (include_views) {
    sql <- table_view_sql(schema)
  } else {
    sql <- table_sql(schema)
  }

  execute_sql(
    database = database,
    server = server,
    sql = sql,
    output = TRUE
  )
}
