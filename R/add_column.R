create_add_column_sql <- function(schema,
                                  table_name,
                                  column_name,
                                  column_type) {
  schema_tbl <- quoted_schema_tbl(schema, table_name)
  glue::glue_sql(
    "ALTER TABLE {schema_tbl} ADD \\
    {DBI::dbQuoteIdentifier(DBI::ANSI(), column_name)} \\
    {DBI::SQL(column_type)} NULL;",
    .con = DBI::ANSI()
  )
}

validate_column_type <- function(sample_value) {
  if (is.function(sample_value)) {
    stop(glue::glue("Invalid input: column_data cannot be a function name. \\
                    For example `character` or `numeric` are invalid, but \\
                    `character()` or `numeric()` are valid argument values"))
  }

  tryCatch(
    {
      if (!is.null(sample_value)) {
        class(sample_value)
      }
    },
    error = function(e) {
      stop("Invalid input: Unable to determine the class of column_type.")
    }
  )
}

clean_new_column_name <- function(table_name, column_name) {
  initial_name <- column_name
  column_name <- substr(column_name, start = 1, stop = 126)
  column_name <- rename_reserved_column(column_name,
                                        table_name,
                                        suffix = "_new")
  column_name <- gsub(pattern = "\\.", replacement = "_", column_name)
  if (column_name != initial_name) {
    warning(glue::glue("Column name {initial_name} is invalid \\
                       using {column_name} instead."), call. = FALSE)
  }
  column_name
}


#' Add a Column to an existing database table.
#'
#' Adds a specified column to a table, with datatype mapped from R
#'  or explicitly specified by the user.
#' Checks if the table exists in the schema before attempting to add the column.
#'
#' @param server Server and instance where SQL Server database found.
#' @param database Database containing the table to which the column
#' will be added.
#' @param schema Name of schema containing the table.
#' @param table_name Name of the table to which the column should be added.
#' @param column_name The name of the column to be added.
#' @param sql_data_type The SQL datatype for the column as a character string.
#' For example `"nvarchar(255)"`
#' (optional, can be inferred from `sample_value` alternatively). Use
#' [`db_table_metadata()`] to see the data types for existing tables/columns
#'  in the database, or refer to MS SQL Server guidance on data types.
#' @param sample_value Existing R data frame column,
#'  or a value that defines the datatype of the column. The input
#'  should be of the correct R class for its type, for example, strings as
#'  character, numbers as numeric, dates as Date, date times as POSIXct/POSIXlt.
#' Optional and superseded by `sql_data_type` if specified. Some example
#' valid inputs: `iris$Species`, `"a sample string"`, `numeric()`,
#'  `as.Date("2024-01-01"`).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Add a Species column to test_iris table
#' add_column(
#'   server = "my_server",
#'   database = "my_database",
#'   schema = "my_schema",
#'   table_name = "test_iris",
#'   column_name = "Species",
#'   column_data = character(0)
#' )
#' }
add_column <- function(server,
                       database,
                       schema,
                       table_name,
                       column_name,
                       sql_data_type = NULL,
                       sample_value = NULL) {
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

  # Ensure the column does not already exist
  if (tolower(column_name) %in% tolower(table_columns)) {
    stop(glue::glue("Column {column_name} already exists \\
                    in {schema}.{table_name}."))
  }

  # Clean invalid column name
  column_name <- clean_new_column_name(table_name, column_name)

  # Infer SQL datatype from R data if not provided
  if (is.null(sql_data_type)) {
    if (is.null(sample_value)) {
      stop(glue::glue("You must provide either `sample_value` \\
           for data type inference or `sql_data_type` directly."))
    }
    validate_column_type(sample_value)
    sql_data_type <- r_to_sql_data_type(sample_value)
  }

  # Create the SQL to add the column
  sql <- create_add_column_sql(schema, table_name, column_name, sql_data_type)

  # Execute the SQL to add the column
  execute_sql(server, database, sql, output = FALSE)

  message(glue::glue(
    "Column {column_name} of type {sql_data_type} \\
    added to {schema}.{table_name}."
  ))
}
