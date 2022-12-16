# If error found on checks drop staging table before stop msg
process_fail <- function(message, db_params) {
  delete_staging_table(db_params, silent = TRUE)
  stop(cli::format_error(message), call. = FALSE)
}

# Column in to load dataframe, but not existing SQL table
missing_col_error <- function(compare_col_df) {
  missing_df <- compare_col_df[compare_col_df$col_issue == "missing sql", ]
  if (nrow(missing_df) > 0) {
    error_message <- glue::glue_data(
      missing_df,
      "Column {column_name} not found in existing table."
    )

    c(error_message, "Use option append_to_existing=FALSE to overwrite")
  }
}

# Column existing SQL table, not in to load df - can still be loaded
missing_col_warning <- function(compare_col_df) {
  missing_df <- compare_col_df[compare_col_df$col_issue == "missing df", ]
  if (nrow(missing_df) > 0) {
    glue::glue_data(
      missing_df,
      "Column {column_name} in existing table not found in dataframe to append."
    )
  }
}

# Column in to load dataframe not compatible with existing SQL table
mismatch_datatype_error <- function(compare_col_df) {
  incompatible_df <- compare_col_df[compare_col_df$col_issue
  == "incompatible", ]
  if (nrow(incompatible_df) > 0) {
    error_message <- glue::glue_data(
      incompatible_df,
      "Column {column_name} existing datatype
      {data_type} not compatible with matching
      dataframe column"
    )

    c(error_message, "Use option append_to_existing=FALSE to overwrite")
  }
}

# Char column in to load dataframe larger than existing SQL table - resize it
resize_datatypes <- function(compare_col_df, db_params) {
  resize_df <- compare_col_df[compare_col_df$col_issue == "resize", ]

  if (nrow(resize_df) > 0) {
    for (row in 1:nrow(resize_df)) {
      alter_sql_character_col(
        db_params, resize_df[row, "column_name"],
        resize_df[row, "df_data_type"]
      )
    }
  }
}

# Compare each row in joined metadata df and check if col present in each
# and if datatypes are compatible
check_columns <- function(compare_col_df) {
  compare_col_df$col_issue <- mapply(
    compatible_cols,
    compare_col_df$data_type,
    compare_col_df$df_data_type
  )
  compare_col_df
}

# Create metadata column name and datatype tbls for existing SQL table
# and the to load df - merge them and compare the differences
compare_columns <- function(db_params, dataframe) {
  sql_metadata <- db_table_metadata(
    db_params$server,
    db_params$database,
    db_params$schema,
    db_params$table_name
  )

  df_metadata <- df_to_metadata(dataframe)

  colnames(df_metadata) <- c("df_column_name", "df_data_type")

  compare_col_df <- merge(
    x = sql_metadata,
    y = df_metadata,
    by.x = "column_name",
    by.y = "df_column_name",
    all = TRUE
  )

  # ID column will not be in to load R df, so ignore this column
  id_col <- paste0(db_params$table_name, "ID")
  compare_col_df <- compare_col_df[compare_col_df$column_name != id_col, ]


  check_columns(compare_col_df)
}


# Main column checking function used when append_to_existing=TRUE
check_existing_table <- function(db_params,
                                 dataframe) {
  compare_col_df <- compare_columns(db_params, dataframe)

  is_missing <- missing_col_error(compare_col_df)
  is_incompatible <- mismatch_datatype_error(compare_col_df)
  is_warning <- missing_col_warning(compare_col_df)

  if (!is.null(is_missing)) {
    process_fail(is_missing, db_params)
  } else if (!is.null(is_incompatible)) {
    process_fail(is_incompatible, db_params)
  } else if (!is.null(is_warning)) {
    warning(cli::format_warning(is_warning), call. = FALSE)
  }


  resize_datatypes(compare_col_df, db_params)

  message(format_message(paste0(
    "Checked existing columns in '",
    db_params$schema, ".", db_params$table_name,
    "' are compatible with those in the dataframe to be loaded."
  )))
}


alter_sql_character_col <- function(db_params,
                                    column_name,
                                    new_char_type) {
  sql <- glue::glue("ALTER TABLE [{db_params$schema}].[{db_params$table_name}]",
    "ALTER COLUMN [{column_name}] {new_char_type};",
    .sep = " "
  )

  execute_sql(
    db_params$server,
    db_params$database,
    sql,
    FALSE
  )
  format_message(glue::glue("Resizing column {column_name}
                            to {new_char_type}."))
}

id_col_name <- function(table_name) {
  table_name <- gsub("_staging_$", "", table_name)
  paste0(table_name, "ID")
}

sql_create_table <- function(schema, table_name, metadata_df) {
  sql <- glue::glue("CREATE TABLE [{schema}].[{table_name}]",
    "([{id_col_name(table_name)}] INT NOT NULL IDENTITY PRIMARY KEY,",
    .sep = " "
  )
  for (row in seq_len(nrow(metadata_df))) {
    if (metadata_df[row, "column_name"] != paste0(table_name, "ID")) {
      column_name <- metadata_df[row, "column_name"]
      data_type <- metadata_df[row, "data_type"]
      sql <- glue::glue(sql, "[{column_name}] {data_type},", .sep = " ")
    }
  }
  glue::glue(substr(sql, 1, nchar(sql) - 1), ");")
}


sql_versioned_table <- function(sql, db_params) {
  # To remove the trailing );
  sql <- substr(sql, 1, nchar(sql) - 2)
  # The versioned table sql
  glue::glue(sql,
    "SysStartTime DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,",
    "SysEndTime DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,",
    "PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime))",
    "WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE =",
    "[{db_params$schema}].[{db_params$table_name}History]));",
    .sep = " "
  )
}

create_staging_table <- function(db_params, dataframe) {
  staging_name <- paste0(db_params$table_name, "_staging_")

  if (check_table_exists(
    db_params$server,
    db_params$database,
    db_params$schema,
    staging_name
  )) {
    drop_table_from_db(
      db_params$server,
      db_params$database,
      db_params$schema,
      staging_name,
      FALSE
    )
  }

  metadata_df <- df_to_metadata(dataframe)

  sql <- sql_create_table(
    db_params$schema,
    staging_name,
    metadata_df
  )

  execute_sql(
    db_params$server,
    db_params$database,
    sql,
    FALSE
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
            schema = db_params$schema,
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

create_insert_sql <- function(db_params, metadata_df) {
  metadata_df <- metadata_df[metadata_df$column_name !=
    paste0(db_params$table_name, "ID"), ]

  column_selects <- glue::glue_collapse(
    glue::glue_data(metadata_df, "[{column_name}]"), ", "
  )


  glue::glue(
    "INSERT INTO [{db_params$schema}].[{db_params$table_name}]",
    "({column_selects}) select {column_selects} from",
    "[{db_params$schema}].[{db_params$table_name}_staging_];",
    .sep = " "
  )
}

populate_table_from_staging <- function(db_params) {
  metadata <- db_table_metadata(
    db_params$server,
    db_params$database,
    db_params$schema,
    paste0(db_params$table_name, "_staging_")
  )

  sql <- create_insert_sql(db_params, metadata)

  execute_sql(db_params$server, db_params$database, sql, FALSE)
  message(format_message(paste0(
    "Table: '", db_params$schema, ".", db_params$table_name,
    "' successfully populated from staging"
  )))
}



delete_staging_table <- function(db_params, silent = FALSE) {
  drop_table_from_db(
    db_params$server,
    db_params$database,
    db_params$schema,
    paste0(db_params$table_name, "_staging_"),
    FALSE,
    TRUE
  )
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
  metadata_df <- db_table_metadata(
    db_params$server,
    db_params$database,
    db_params$schema,
    paste0(db_params$table_name, "_staging_")
  )
  sql <- sql_create_table(
    db_params$schema,
    db_params$table_name,
    metadata_df
  )

  if (db_params$versioned_table) {
    sql <- sql_versioned_table(sql, db_params)
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
  if (!check_table_exists(
    server,
    database,
    schema,
    table_name
  )) {
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
      server,
      database,
      schema,
      table_name,
      TRUE,
      silent = TRUE
    )
    # Create the new one
    create_table(db_params, silent = TRUE)
  }

  # Populate the staging table using batch import of rows from R dataframe
  populate_staging_table(db_params,
    dataframe = dataframe
  )
  # Then populate the target table from staging
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
