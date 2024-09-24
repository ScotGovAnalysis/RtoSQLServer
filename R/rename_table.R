# Create the SQL to rename a table
rename_table_sql <- function(schema,
                             old_table_name,
                             new_table_name) {
  schema_tbl_old <- quoted_schema_tbl(schema, old_table_name)
  glue::glue_sql(
    "EXEC sp_rename '{schema_tbl_old}', \\
    {DBI::dbQuoteIdentifier(DBI::ANSI(), new_table_name)}, 'OBJECT';",
    .con = DBI::ANSI()
  )
}

#' Rename a table in the database
#'
#' Renames a specified table. The function checks if the table exists
#' in the schema before attempting to rename it.
#' In MS SQL Server, this is done using `EXEC sp_rename`.
#'
#' @param server Server and instance where SQL Server database found.
#' @param database Database containing the table to be renamed.
#' @param schema Name of the schema containing the table.
#' @param old_table_name The current name of the table to be renamed.
#' @param new_table_name The new name for the table.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Rename test_iris table to test_iris_renamed
#' rename_table(
#'   server = "my_server",
#'   database = "my_database",
#'   schema = "my_schema",
#'   old_table_name = "test_iris",
#'   new_table_name = "test_iris_renamed"
#' )
#' }
rename_table <- function(server,
                         database,
                         schema,
                         old_table_name,
                         new_table_name) {
  # Check if the old table exists
  if (!check_table_exists(
    server,
    database,
    schema,
    old_table_name,
    include_views = FALSE
  )) {
    stop(glue::glue(
      "Table: {schema}.{old_table_name} does not exist in the database. Is \\
      {schema}.{old_table_name} a view instead of a table?"
    ), call. = FALSE)
  }

  # Ensure the new table name doesn't already exist
  if (check_table_exists(
    server,
    database,
    schema,
    new_table_name,
    include_views = FALSE
  )) {
    stop(glue::glue(
      "Table: {schema}.{new_table_name} already exists in the database."
    ), call. = FALSE)
  }

  # Clean the new table name
  new_table_name <- clean_table_name(new_table_name)

  # Create the SQL (ExEC sp_rename)
  sql <- rename_table_sql(
    schema,
    old_table_name,
    new_table_name
  )

  execute_sql(server, database, sql, output = FALSE)

  message(glue::glue(
    "Table {old_table_name} renamed to {new_table_name} in schema {schema}."
  ))
}
