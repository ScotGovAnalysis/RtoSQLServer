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
        "Failed to create connection to database:",
        "{database} on server: {server}",
        "\n{cond}",
        .sep = " "
      ))
    }
  )
}




get_db_tables <- function(server, database) {
  sql <- "SELECT SCHEMA_NAME(t.schema_id) AS 'Schema',
  t.name AS 'Name'
  FROM sys.tables t"
  data <- execute_sql(
    database = database,
    server = server,
    sql = sql,
    output = TRUE
  )
  data
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
  data.frame(
    column_name = names(col_types), data_type = unname(col_types),
    stringsAsFactors = FALSE
  )
}


r_to_sql_data_type <- function(col_v) {
  r_data_type <- class(col_v)
  if (r_data_type %in% c("character", "factor")) {
    col_v <- as.character(col_v) # to ensure factor cols are character
    max_string <- max(nchar(col_v), na.rm = TRUE)
  }
  switch(r_data_type,
    "numeric" = "float",
    "logical" = "bit",
    "character" = r_to_sql_character_sizes(max_string),
    "factor" = r_to_sql_character_sizes(max_string),
    "POSIXct" = "datetime2(3)",
    "POSIXlt" = "datetime2(3)",
    "Date" = "datetime2(3)",
    "integer" = "int"
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
                               table_name) {
  all_tables <- get_db_tables(database = database, server = server)
  # return TRUE if exists or else false
  nrow(all_tables[all_tables$Schema == schema &
    all_tables$Name == table_name, ]) == 1
}

# Prevent SQL injection with quoted schema table name construction
quoted_schema_tbl <- function(schema, table_name) {
  DBI::dbQuoteIdentifier(
    DBI::ANSI(),
    DBI::Id(schema = schema, table = table_name)
  )
}
