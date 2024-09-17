create_drop_column_sql <- function(schema, table_name, column_name) {
  glue::glue_sql(
    "ALTER TABLE {quoted_schema_tbl(schema, table_name)} DROP COLUMN \\
    {DBI::dbQuoteIdentifier(DBI::ANSI(), column_name)};",
    .con = DBI::ANSI()
  )
}


#' Drop a Column from an existing database table.
#'
#' Drops a specified column from a table.
#' Checks if the table exists in the schema and if the column is present in the
#' table before attempting to drop the column.
#'
#' @param server Server and instance where SQL Server database found.
#' @param database Database containing the table with column to drop.
#' @param schema Name of schema containing the table.
#' @param table_name Name of the table from which the column should be dropped.
#' @param column_name The name of the column to be dropped.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Drop the Species column from test_iris table
#' drop_column(server = "my_server",
#' database = "my_database",
#' schema = "my_schema",
#' table_name = "test_iris",
#' column_name = "Species")
#' }
drop_column <- function(server, database, schema, table_name, column_name) {
  if (!check_table_exists(
    server,
    database,
    schema,
    table_name,
    include_views = FALSE
  )) {
    stop(glue::glue(
      "Table: {schema}.{table_name} does not exist in the database. Is \\
      {schema}.{table_name} a view instead of a table?"
    ))
  }

  table_columns <- db_table_metadata(
    server,
    database,
    schema,
    table_name
  )$column_name

  if (!column_name %in% table_columns) {
    stop(glue::glue("Column {column_name} does not exist in \\
                    {schema}.{table_name}"))
  }
  sql <- create_drop_column_sql(schema, table_name, column_name)

  execute_sql(server, database, sql, output = FALSE)

  message(glue::glue(
    "Column {column_name} dropped from {schema}.{table_name}"
  ))
}
