# Create the SQL to rename a column in a table
rename_column_sql <- function(schema,
                              table_name,
                              old_column_name,
                              new_column_name) {
  schema_tbl <- quoted_schema_tbl(schema, table_name)
  glue::glue_sql(
    "EXEC sp_rename '{schema_tbl}.\\
    {DBI::dbQuoteIdentifier(DBI::ANSI(), old_column_name)}', \\
    {DBI::dbQuoteIdentifier(DBI::ANSI(), new_column_name)}, 'COLUMN';",
    .con = DBI::ANSI()
  )
}

#' Rename a column in an existing database table.
#'
#' Renames a specified column in a table. The function checks if the table
#' and the column exist in the schema before attempting to rename the column.
#'
#' @param server Server and instance where SQL Server database found.
#' @param database Database containing the table in which the column
#' belongs.
#' @param schema Name of the schema containing the table.
#' @param table_name Name of the table containing the column.
#' @param old_column_name The current name of the column to be renamed.
#' @param new_column_name The new name for the column.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Rename Species column to SpeciesRenamed in test_iris table
#' rename_column(
#'   server = "my_server",
#'   database = "my_database",
#'   schema = "my_schema",
#'   table_name = "test_iris",
#'   old_column_name = "Species",
#'   new_column_name = "SpeciesRenamed"
#' )
#' }
rename_column <- function(server,
                          database,
                          schema,
                          table_name,
                          old_column_name,
                          new_column_name) {
  # Check if the table exists
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
    ), call. = FALSE)
  }

  # Get current table metadata
  table_columns <- db_table_metadata(
    server,
    database,
    schema,
    table_name
  )$column_name

  # Ensure the old column exists
  if (!tolower(old_column_name) %in% tolower(table_columns)) {
    stop(glue::glue("Column {old_column_name} does not exist \\
                    in {schema}.{table_name}."), call. = FALSE)
  }

  # Ensure the new column name doesn't already exist
  if (tolower(new_column_name) %in% tolower(table_columns)) {
    stop(glue::glue("Column {new_column_name} already exists \\
                    in {schema}.{table_name}."), call. = FALSE)
  }

  # Clean the new column name
  new_column_name <- clean_new_column_name(table_name, new_column_name)

  # Create the SQL to rename the column using the internal function
  sql <- rename_column_sql(
    schema,
    table_name,
    old_column_name,
    new_column_name
  )

  # Execute the SQL to rename the column
  execute_sql(server, database, sql, output = FALSE)

  message(glue::glue(
    "Column {old_column_name} renamed to {new_column_name} \\
    in {schema}.{table_name}."
  ))
}
