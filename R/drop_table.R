# sql to check if versioned table
create_check_sql <- function(schema, table_name) {
  glue::glue_sql(
    "select name, temporal_type_desc from sys.tables where name = ",
    "{table_name} and schema_name(schema_id) = {schema};",
    .con = DBI::ANSI()
  )
}


# create versioned table sql
create_drop_sql_versioned <- function(schema, table_name) {
  history_table <- quoted_schema_tbl(schema, glue::glue(table_name, "History"))
  glue::glue_sql("ALTER TABLE {`quoted_schema_tbl(schema, table_name)`} ",
    "SET ( SYSTEM_VERSIONING = OFF );",
    "DROP TABLE {`quoted_schema_tbl(schema, table_name)`};",
    "DROP TABLE {`history_table`};",
    .con = DBI::ANSI()
  )
}

# create non versioned table sql
create_drop_sql_nonversioned <- function(schema, table_name) {
  glue::glue_sql("DROP TABLE {`quoted_schema_tbl(schema, table_name)`};",
    .con = DBI::ANSI()
  )
}

# Ensure a versioned table based on metadata
is_versioned <- function(check_df) {
  check_df[["temporal_type_desc"]] == "SYSTEM_VERSIONED_TEMPORAL_TABLE"
}

# Derive the drop sql depending on if versioned (check first)
create_drop_sql <- function(server,
                            database,
                            schema,
                            table_name,
                            versioned_table) {
  if (versioned_table) {
    # Check is versioned table regardless of versioned_table input arg
    check_sql <- create_check_sql(schema, table_name)
    check_df <- execute_sql(
      server = server,
      database = database,
      sql = check_sql,
      output = TRUE
    )
    if (is_versioned(check_df)) {
      drop_sql <- create_drop_sql_versioned(schema, table_name)
    } else { # if not actually versioned:
      drop_sql <- create_drop_sql_nonversioned(schema, table_name)
    }
  } else { # If versioned_table argument is FALSE
    drop_sql <- create_drop_sql_nonversioned(schema, table_name)
  }
  return(drop_sql)
}

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

  # Create drop sql

  drop_sql <- create_drop_sql(
    server,
    database,
    schema,
    table_name,
    versioned_table
  )

  # Run drop sql

  execute_sql(
    server,
    database,
    drop_sql,
    output = FALSE
  )

  # Output message if required
  if (!silent) {
    message(glue::glue("Table: {schema}.{table_name}",
      "successfully deleted.",
      .sep = " "
    ))
  }
}
