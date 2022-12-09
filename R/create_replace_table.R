check_existing_table <- function(db_params,
                                 dataframe) {
  sql_columns <- db_table_metadata(
    db_params$server,
    db_params$database,
    db_params$schema,
    db_params$table_name
  )

  for (col_name in colnames(dataframe)) {

    # First check for columns that do not exist at all in target
    if (!col_name %in% sql_columns$ColumnName) {
      delete_staging_table(db_params, silent = TRUE)
      stop(format_message(paste0(
        "Column '", col_name,
        "' not found in existing SQL Server table '",
        table_name, "'- use option append_to_existing=FALSE if wish to replace"
      )))
    }

    # If column exists, then check if datatypes are compatible

    df_col_type <- r_to_sql_datatype(dataframe[[col_name]])
    sql_col_type <- sql_columns[sql_columns["ColumnName"] ==
      col_name, "DataType"]

    # - may be incompatible types e.g. numeric and char
    # - may need to resize existing database table nvarchar col
    # - or may be already compatible as existing database nvarchar col max
    # - larger than data in df to load
    if (sql_col_type != df_col_type) {
      # If char cols of different sizes might still be compatible:
      mismatch_type <- compatible_character_cols(sql_col_type, df_col_type)
      if (mismatch_type == "incompatible") {
        delete_staging_table(db_params, silent = TRUE)
        stop(format_message(paste0(
          "Column '", col_name, "' datatype: '",
          df_col_type, "' does not match existing type '", sql_col_type, "'."
        )))
      } else if (mismatch_type == "resize") {
        message(format_message(paste0(
          "Resizing existing column '",
          col_name, "' from ", sql_col_type, " to ", df_col_type
        )))
        alter_sql_character_col(db_params,
          column_name = col_name,
          new_char_type = df_col_type
        )
      }
    }
  }
  message(format_message(paste0(
    "Checked existing columns in '",
    db_params$schema, ".", db_params$table_name,
    "' are compatible with those in the dataframe to be loaded."
  )))
}


alter_sql_character_col <- function(db_params,
                                    column_name,
                                    new_char_type) {
  sql <- paste0(
    "ALTER TABLE [", db_params$schema, "].[", db_params$table_name,
    "] ALTER COLUMN [", column_name, "] ", new_char_type, ";"
  )
  execute_sql(
    dbparams$server,
    db_params$database,
    sql,
    FALSE
  )
}

create_staging_table <- function(db_params, dataframe) {
  tables <- get_db_tables(db_params$server, db_params$database)
  if (nrow(tables[tables$Schema == schema & tables$Name ==
    paste0(db_params$table_name, "_staging_"), ]) > 0) {
    drop_table_from_db(
      db_params$server,
      db_params$database,
      db_params$schema,
      paste0(db_params$table_name, "_staging_"),
      FALSE
    )
  }
  sql <- paste0(
    "CREATE TABLE [", db_params$schema, "].[",
    db_params$table_name, "_staging_] (",
    db_params$table_name, "ID INT NOT NULL IDENTITY PRIMARY KEY,"
  )
  for (column_name in colnames(dataframe)) {
    data_type <- r_to_sql_datatype(dataframe[[column_name]])
    sql <- paste0(sql, " [", column_name, "] ", data_type, ", ")
  }
  sql <- paste0(substr(sql, 1, nchar(sql) - 2), ");")
  execute_sql(
    server = db_params$server,
    db_params$database,
    sql = sql,
    output = FALSE
  )
  message(format_message(
    paste0(
      "Table: '", db_params$schema, ".", db_params$table_name, "_staging_",
      "' successfully created in database: '",
      db_params$database,
      "' on server '",
      db_params$server, "'"
    )
  ))
}


get_df_batches <- function(dataframe, batch_size) {
  batch_starts <- seq(1, nrow(dataframe), batch_size)
  if (nrow(dataframe) > batch_size) {
    batch_ends <- seq(batch_size, nrow(dataframe), batch_size)
    if (tail(batch_ends, 1) < nrow(dataframe)) {
      batch_ends <- c(batch_ends, nrow(dataframe))
    }
  }
  else {
    batch_ends <- c(nrow(dataframe))
  }

  list(batch_starts = batch_starts, batch_ends = batch_ends)
}


populate_staging_table <- function(db_params,
                                   dataframe,
                                   batch_size = 5e5) {
  connection <- create_sqlserver_connection(
    db_params$server,
    db_params$database
  )
  batch_list <- get_df_batches(dataframe = dataframe, batch_size = batch_size)
  message(format_message(paste(
    "Loading to staging in", length(batch_list$batch_starts),
    "batches of up to", format(batch_size,
      scientific =
        FALSE
    ), "rows..."
  )))
  for (i in seq_along(batch_list$batch_starts)) {
    batch_start <- batch_list$batch_starts[[i]]
    batch_end <- batch_list$batch_ends[[i]]
    load_df <- data.frame(dataframe[batch_start:batch_end, ])
    tryCatch(
      {
        DBI::dbWriteTable(connection,
          name = DBI::Id(
            schema = schema,
            table = paste0(db_params$table_name, "_staging_")
          ), value = load_df,
          overwrite = FALSE, append = TRUE
        )
      },
      error = function(cond) {
        stop(format_message(paste0("Failed to write staging
                    data to database.\nOriginal error message: ", cond)))
      }
    )
    message(format_message(paste(
      "Loaded rows", format(batch_start, scientific = FALSE),
      "-", format(batch_end, scientific = FALSE), "of",
      tail(batch_list$batch_ends, 1)
    )))
  }
  DBI::dbDisconnect(connection)
}


populate_table_from_staging <- function(db_params) {
  metadata <- db_table_metadata(
    db_params$server,
    db_params$database,
    db_params$schema,
    paste0(db_params$table_name, "_staging_")
  )
  column_string <- ""
  for (row in seq_len(nrow(metadata))) {
    if (metadata[row, "ColumnName"] != paste0(db_params$table_name, "ID")) {
      column_name <- metadata[row, 1]
      column_string <- paste0(column_string, " [", column_name, "], ")
    }
  }
  column_string <- substr(column_string, 1, nchar(column_string) - 2)
  sql <- paste0(
    "INSERT INTO [", db_params$schema, "].[", db_params$table_name,
    "] (", column_string, ") select ",
    column_string, " from [", schema, "].[",
    db_params$table_name, "_staging_];"
  )
  execute_sql(db_params$server, db_params$database, sql, FALSE)
  message(format_message(paste0(
    "Table: '", db_params$schema, ".", db_params$table_name,
    "' successfully populated from staging"
  )))
}



delete_staging_table <- function(db_params, silent = FALSE) {
  connection <- create_sqlserver_connection(
    db_params$server,
    db_params$database
  )
  tryCatch(
    {
      odbc::dbRemoveTable(conn = connection, DBI::Id(
        schema = db_params$schema,
        table = paste0(db_params$table_name, "_staging_")
      ))
    },
    error = function(cond) {
      stop(format_message(paste0(
        "Failed to delete staging table: '", db_params$table_name,
        "' from database: '", db_params$database, "' on server: '",
        db_params$server, "'\nOriginal error message: ", cond
      )))
    }
  )
  DBI::dbDisconnect(connection)
  if (!silent) {
    message(format_message(paste0(
      "Staging table: '", db_params$schema, ".",
      db_params$table_name, "_staging_",
      "' successfully deleted from database: '",
      db_params$database, "' on server '", db_params$server, "'"
    )))
  }
}



create_table <- function(db_params, silent = FALSE) {
  metadata <- db_table_metadata(
    db_params$server,
    db_params$database,
    db_params$schema,
    paste0(db_params$table_name, "_staging_")
  )
  sql <- paste0(
    "CREATE TABLE [", db_params$schema, "].[", db_params$table_name, "] (",
    db_params$table_name, "ID INT NOT NULL IDENTITY PRIMARY KEY,"
  )
  for (row in seq_len(nrow(metadata))) {
    if (metadata[row, "ColumnName"] != paste0(db_params$table_name, "ID")) {
      column_name <- metadata[row, "ColumnName"]
      data_type <- metadata[row, "DataType"]
      sql <- paste0(sql, " [", column_name, "] ", data_type, ", ")
    }
  }
  if (db_params$versioned_table) {
    sql <- paste0(
      sql,
      "SysStartTime DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL, ",
      "SysEndTime DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL, ",
      "PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)) ",
      "WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [",
      db_params$schema, "].[", db_params$table_name, "History]));"
    )
  }
  else {
    sql <- paste0(substr(sql, 1, nchar(sql) - 2), ");")
  }
  execute_sql(
    db_params$server,
    db_params$database,
    sql,
    FALSE
  )
  if (!silent) {
    message(format_message(paste0(
      "Table: '", paste0(db_params$schema, ".", db_params$table_name),
      "' successfully created in database: '", db_params$database,
      "' on server '", db_params$server, "'"
    )))
  }
}

clean_table_name <- function(table_name) {
  # Replace - with _
  new_name <- gsub("-", "_", table_name, ignore.case = TRUE)
  # Replace spaces with _
  new_name <- gsub("\\s", "_", new_name, ignore.case = TRUE)
  # Remove any characters not character, number underscore
  new_name <- gsub("[^0-9a-z_]", "", new_name, ignore.case = TRUE)
  # Advise if changing target table name
  if (new_name != table_name) {
    message(format_message(paste0(
      "Cannot name a table'", table_name,
      "' replacing with name '", new_name,
      "' (see ODBC table name limitations)"
    )))
  }
  return(new_name)
}

rename_reserved_column <- function(column_name, table_name) {
  if (tolower(column_name) %in% c(
    paste0(table_name, "key"),
    paste0(table_name, "versionkey"),
    "sysstarttime", "sysendtime"
  )) {
    paste0(column_name, "_old")
  } else {
    column_name
  }
}

clean_column_names <- function(input_df, table_name) {
  # Get column names as vector
  column_names <- colnames(input_df)
  # Truncate any names that have > 128 characters
  column_names <- sapply(column_names, substr, start = 1, stop = 128)
  # Rename any column names that are SQL Server reserved
  column_names <- sapply(column_names, rename_reserved_column, table_name)
  # . is exceptable in R dataframe column name not good for SQL select
  column_names <- unlist(lapply(column_names, gsub,
    pattern = "\\.",
    replacement = "_"
  ))
  # Assign and return df
  colnames(input_df) <- column_names
  input_df
}


#' Write an R dataframe to SQL Server table optionally with system
#' versioning on.
#'
#' @param server Server instance where SQL Server database running.
#' @param database Name of SQL Server database where table will be written.
#' @param schema Name of schema in SQL Server database where table will
#' be created.
#' @param table_name Name of table to be created in SQL Server database.
#' @param dataframe Source R dataframe that will be written to SQL Server
#' database.
#' @param append_to_existing Boolean if TRUE then rows will be appended to
#' existing
#'  database table (if exists). Default FALSE.
#' @param batch_size Source R dataframe rows will be loaded into a staging SQL
#'  Server table in batches of this many rows at a time.
#' @param versioned_table Create table with SQL Server system versioning.
#' Defaults to FALSE. If table already exists in DB will not
#' change existing versioning status.
#'
#' @importFrom utils tail
#' @export
#'
#' @examples
#' \dontrun{
#' write_dataframe_to_db(
#'   server = "my_server",
#'   schema = "my_schema",
#'   table_name = "output_table",
#'   dataframe = my_df
#' )
#' }
write_dataframe_to_db <- function(server,
                                  database,
                                  schema,
                                  table_name,
                                  dataframe,
                                  append_to_existing = FALSE,
                                  batch_size = 1e5,
                                  versioned_table = FALSE) {

  # Put db params in list for convernience in internal fn calls
  db_params <- list(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name,
    append_to_existing = append_to_existing,
    batch_size = batch_size,
    versioned_table = versioned_table
  )

  start_time <- Sys.time()
  # Clean table_name in case special characters included
  table_name <- clean_table_name(table_name)
  # Clean df column names
  dataframe <- clean_column_names(dataframe, table_name)
  # Create staging table
  create_staging_table(db_params, dataframe)
  # Check if target table already exists
  tables <- get_db_tables(server = server, database = database)
  # If does bot exist create it
  if (nrow(tables[tables$Schema == schema & tables$Name == table_name, ])
  == 0) {
    create_table(db_params)
    # If exists and appending then check existing columns
  } else if (append_to_existing) {
    check_existing_table(db_params, dataframe)
    # If not appending and exists then inform that will be overwritten
  } else {
    (message(format_message(paste0(
      "Existing table '", schema, "'.'", table_name,
      "' will be over-written."
    ))))
    # Drop the existing table
    drop_table_from_db(
      server = server,
      database = database,
      schema = schema,
      table_name = table_name,
      versioned_table = TRUE,
      silent = TRUE
    )
    # Create the new one
    create_table(db_params, silent = TRUE)
  }

  # Populte the staging table using batch import of rows from R dataframe
  populate_staging_table(db_params,
    dataframe = dataframe
  )
  # Then populate the target table from staging, truncating
  # it first of existing rows
  tryCatch(
    {
      populate_table_from_staging(db_params)
    },
    error = function(cond) {
      stop(format_message(paste0(
        "Failed to write dataframe to database: '",
        database, "'\nOriginal error message: ", cond
      )))
    }
  )
  # Drop the staging table and finished
  delete_staging_table(db_params)
  end_time <- Sys.time()
  message(format_message(paste(
    "Loading completed in",
    round(difftime(end_time, start_time,
      units = "mins"
    )[[1]], 2),
    " minutes."
  )))
}
