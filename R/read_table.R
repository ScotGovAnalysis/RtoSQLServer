table_select_list <- function(columns) {
  if (is.null(columns)) {
    "*"
  } else {
    glue::glue_collapse(glue::glue("[{columns}]"), sep = ", ")
  }
}

create_read_sql <- function(select_list, schema, table_name) {
  glue::glue(
    "SELECT {select_list}",
    "FROM [{schema}].[{table_name}];",
    .sep = " "
  )
}


#' Read a SQL Server table into an R dataframe,
#'
#' @param server Server instance where SQL Server database running.
#' @param database Database containing table to read.
#' @param schema Name of database schema containing table to read.
#' @param table_name Name of table in database to read.
#' @param columns Optional vector of column names to select.
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
#'   columns = c("column1", "column2")
#' )
#' }
read_table_from_db <- function(database,
                               server,
                               schema,
                               table_name,
                               columns = NULL) {
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
    select_list,
    schema,
    table_name
  )

  execute_sql(
    database = database, server =
      server, sql = read_sql, output = TRUE
  )
}
