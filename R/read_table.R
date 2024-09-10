table_select_list <- function(server,
                              database,
                              schema,
                              table_metadata,
                              table_name,
                              columns,
                              include_pk) {
  # all column names found in existing table
  existing_cols <- table_metadata$column_name

  # Get the primary key column name
  pk <- get_pk_name(server, database, schema, table_name)

  # if user specified column list - add pk to it if they've set to TRUE
  if (!is.null(columns)) {
    if (include_pk) {
      if (!pk %in% columns) {
        columns <- c(pk, columns)
      }
    }
    # check the columns all exist, if not fail
    not_exist_cols <- setdiff(columns, existing_cols)
    if (length(not_exist_cols) > 0) {
      stop(glue::glue(
        "Column {not_exist_cols} not found in {schema}.{table_name}."
      ), call. = FALSE)
    } else {
      return(columns)
    }

    # where no user specified list of columns
  } else {
    if (!include_pk && !is.null(pk)) {
      return(existing_cols[existing_cols != pk])
    } else {
      return(existing_cols)
    }
  }
}


format_filter <- function(connection, filter_stmt) {
  sql <- dbplyr::translate_sql(!!rlang::parse_expr(filter_stmt),
    con = connection
  )
  sql <- as.character(sql)
}

# Cast datetime2 columns to datetime- workaround due to old ODBC client drivers
col_select <- function(column_name, datetime2_cols_to_cast) {
  if (column_name %in% datetime2_cols_to_cast) {
    return(glue::glue_sql("CAST({`column_name`} AS datetime) ",
      "AS {`column_name`}",
      .con = DBI::ANSI()
    ))
  } else {
    return(glue::glue_sql("{`column_name`}", .con = DBI::ANSI()))
  }
}

cols_select_format <- function(select_list,
                               table_metadata,
                               cast_datetime2) {
  datetime2_cols_to_cast <- NULL
  if (cast_datetime2) {
    # Need to know the datetime2 cols to cast them
    datetime2_cols_to_cast <- table_metadata[table_metadata$data_type ==
      "datetime2", "column_name"]
    if (length(datetime2_cols_to_cast) == 0) {
      datetime2_cols_to_cast <- NULL
    }
  }
  formatted_cols <- vapply(select_list,
    col_select,
    FUN.VALUE = character(1),
    datetime2_cols_to_cast = datetime2_cols_to_cast
  )
  glue::glue_sql_collapse(formatted_cols, sep = ", ")
}


create_read_sql <- function(connection,
                            schema,
                            select_list,
                            table_name,
                            table_metadata,
                            filter_stmt,
                            cast_datetime2) {
  column_sql <- cols_select_format(select_list, table_metadata, cast_datetime2)

  initial_sql <- glue::glue_sql(
    "SELECT {column_sql} FROM {`quoted_schema_tbl(schema, table_name)`}",
    .con = connection
  )
  if (!is.null(filter_stmt)) {
    filter_stmt <- format_filter(connection, filter_stmt)
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
#' @param filter_stmt Optional filter statement to only read a subset of
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


#' @param include_pk Whether to include primary key column in output dataframe.
#' A primary key  column is added automatically when a table is loaded into the
#' database using `create_replace_table` as <table_name>ID. Defaults to FALSE.
#'
#' @return Dataframe of table.
#' @export
#'
#' @examples
#' \dontrun{
#' read_table_from_db(
#'   server = "my_server",
#'   database = "my_database",
#'   schema = "my_schema",
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
                               filter_stmt = NULL,
                               include_pk = FALSE,
                               cast_datetime2 = TRUE) {
  if (!check_table_exists(
    server,
    database,
    schema,
    table_name
  )) {
    stop(glue::glue(
      "Table: {schema}.{table_name} does not exist in the database."
    ), call. = FALSE)
  }

  # Use a genuine connection, so the filter translation SQL is correct
  connection <- create_sqlserver_connection(
    server = server,
    database = database
  )

  table_metadata <- db_table_metadata(
    server,
    database,
    schema,
    table_name
  )

  select_list <- table_select_list(
    server,
    database,
    schema,
    table_metadata,
    table_name,
    columns,
    include_pk
  )

  read_sql <- create_read_sql(
    connection,
    schema,
    select_list,
    table_name,
    table_metadata,
    filter_stmt,
    cast_datetime2
  )
  DBI::dbDisconnect(connection)
  message(glue::glue("Read SQL statement:\n{read_sql}"))
  execute_sql(
    database = database, server =
      server, sql = read_sql, output = TRUE
  )
}
