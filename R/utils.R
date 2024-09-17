create_sqlserver_connection <- function(server, database, timeout = 10) {
  tryCatch(
    {
      odbc::dbConnect(
        odbc::odbc(),
        Driver = "SQL Server",
        Trusted_Connection = "True",
        DATABASE = database,
        SERVER = server,
        timeout = timeout
      )
    },
    error = function(cond) {
      stop(glue::glue(
        "Failed to create connection to database: \\
        {database} on server: {server} \\
        \n{cond}"
      ), call. = FALSE)
    }
  )
}


r_to_sql_character_sizes <- function(max_string) {
  max_string <- as.numeric(max_string)
  if (max_string <= 50) {
    "nvarchar(50)"
  } else if (max_string > 50 && max_string <= 255) {
    "nvarchar(255)"
  } else if (max_string > 255 && max_string <= 4000) {
    "nvarchar(4000)"
  } else {
    "nvarchar(max)"
  }
}

df_to_metadata <- function(dataframe) {
  col_types <- sapply(dataframe, r_to_sql_data_type)
  df <- data.frame(
    column_name = names(col_types), data_type = unname(col_types),
    stringsAsFactors = FALSE
  )
  # db_table_metdata stored procedure does not specify size of datetime2 cols
  df[df$data_type == "datetime2(3)", "data_type"] <- "datetime2"
  df
}


r_to_sql_data_type <- function(col_v) {
  r_data_type <- class(col_v)[1]
  if (r_data_type %in% c("character", "factor", "ordered")) {
    col_v <- as.character(col_v) # to ensure factor cols are character
    max_string <- max(nchar(col_v), na.rm = TRUE)
  }
  switch(r_data_type,
    "numeric" = "float",
    "logical" = "bit",
    "character" = r_to_sql_character_sizes(max_string),
    "factor" = r_to_sql_character_sizes(max_string),
    "ordered" = r_to_sql_character_sizes(max_string),
    "POSIXct" = "datetime2(3)",
    "POSIXlt" = "datetime2(3)",
    "Date" = "datetime2(3)",
    "difftime" = "time",
    "integer" = "int",
    "nvarchar(255)"
  )
}

get_nvarchar_size <- function(input_char_type) {
  sub("\\)", "", unlist(strsplit(input_char_type, "\\("))[2])
}


compatible_cols <- function(existing_col_type,
                            to_load_col_type) {
  # If existing is na then column not in sql db
  if (is.na(existing_col_type)) {
    return("missing sql")
  } else if (is.na(to_load_col_type)) {
    return("missing df")
  } else if (existing_col_type == to_load_col_type) {
    return("compatible")
  } else if (!(grepl("nvarchar", existing_col_type) &&
    grepl("nvarchar", to_load_col_type)
  )) {
    return("incompatible")
  } else {
    # Extract the nvarchar column size, e.g. 255 from nvarchar(255)
    existing_col_size <- get_nvarchar_size(existing_col_type)
    to_load_col_size <- get_nvarchar_size(to_load_col_type)
    if (existing_col_size == "max") {
      return("compatible") # If existing is max then to load will be fine
    } else if (to_load_col_size == "max") {
      return("resize") # If existing not max but to load is then must resize
    } else if (as.numeric(to_load_col_size)
    <= as.numeric(existing_col_size)) {
      return("compatible") # If neither is max, but existing greater
      # than or equal to load then will be fine
    } else {
      return("resize") # If neither is max, but existing smaller
      # than to load then must resize
    }
  }
}


# TRUE or FALSE test for table in schema
check_table_exists <- function(server,
                               database,
                               schema,
                               table_name,
                               include_views = TRUE) {
  all_tables <- show_schema_tables(
    database = database,
    server = server,
    schema = schema,
    include_views = include_views
  )
  # return TRUE if exists or else false
  nrow(all_tables[all_tables$table == table_name, ]) == 1
}

# Prevent SQL injection with quoted schema table name construction
quoted_schema_tbl <- function(schema, table_name) {
  DBI::dbQuoteIdentifier(
    DBI::ANSI(),
    DBI::Id(schema = schema, table = table_name)
  )
}

# Get primary key
get_pk_name <- function(server,
                        database,
                        schema,
                        table_name) {
  sql <- glue::glue_sql(
    "select C.COLUMN_NAME FROM \\
    INFORMATION_SCHEMA.TABLE_CONSTRAINTS T \\
    JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C \\
    ON C.CONSTRAINT_NAME = T.CONSTRAINT_NAME \\
    WHERE  C.TABLE_NAME = {table_name} \\
    AND T.CONSTRAINT_SCHEMA = {schema} \\
    AND T.CONSTRAINT_TYPE = 'PRIMARY KEY';",
    .con = DBI::ANSI()
  )
  df <- execute_sql(server, database, sql, output = TRUE)
  if (length(df$COLUMN_NAME) > 0) {
    df$COLUMN_NAME
  } else {
    NULL
  }
}

# format filter used in read and delete table rows functions
format_filter <- function(filter_stmt) {
  sql <- dbplyr::translate_sql(!!rlang::parse_expr(filter_stmt),
    con = dbplyr::simulate_mssql()
  )
  sql <- as.character(sql)
  # it quotes identifiers with `` which is invalid in MS SQL Server
  gsub("`", "\"", sql)
}

# sql to check if versioned table
create_check_sql <- function(schema, table_name) {
  glue::glue_sql(
    "select name, temporal_type_desc from sys.tables where name = \\
    {table_name} and schema_name(schema_id) = {schema};",
    .con = DBI::ANSI()
  )
}

# Check is versioned
is_versioned <- function(server, database, schema, table_name) {
  check_sql <- create_check_sql(schema, table_name)
  check_df <- execute_sql(server, database, check_sql, output = TRUE)
  check_df[["temporal_type_desc"]] == "SYSTEM_VERSIONED_TEMPORAL_TABLE"
}
