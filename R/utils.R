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
      stop(format_message(paste0(
        "Failed to create connection to database: '",
        database, "' on server: '",
        server, "'\nOriginal error message: '", cond, "'"
      )))
    }
  )
}


db_table_metadata <- function(server, database, schema, table_name) {
  sql <- paste0("SET NOCOUNT ON;
                DECLARE	@table_catalog nvarchar(128) = '", database, "',
                @table_schema nvarchar(128) = '", schema, "',
                @table_name nvarchar(128) = '", table_name, "';
                DECLARE @sql_statement nvarchar(2000),
                @param_definition nvarchar(500),
                @column_name nvarchar(128),
                @data_type nvarchar(128),
                @null_count int,
                @distinct_values int,
                @minimum_value nvarchar(225),
                @maximum_value nvarchar(225);
                DECLARE @T1 AS TABLE	(ColumnName nvarchar(128),
                DataType nvarchar(128),
                NullCount int,
                DistinctValues int,
                MinimumValue nvarchar(255),
                MaximumValue nvarchar(255));
                INSERT INTO @T1 (ColumnName, DataType)
                SELECT	COLUMN_NAME,
                REPLACE(CONCAT(DATA_TYPE, '(',
                CHARACTER_MAXIMUM_LENGTH, ')'), '()', '')
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE	TABLE_CATALOG = @table_catalog
                AND TABLE_SCHEMA = @table_schema
                AND TABLE_NAME = @table_name;
                DECLARE column_cursor CURSOR
                FOR SELECT ColumnName, DataType FROM @T1;
                OPEN column_cursor;
                FETCH NEXT FROM column_cursor
                INTO @column_name, @data_type;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                SET @sql_statement =
                CONCAT(N'SET @null_countOUT =
                (SELECT COUNT(*)
                FROM [', @table_catalog, '].
                [', @table_schema, '].[', @table_name, ']
                WHERE [', @column_name, '] IS NULL)
                SET @distinct_valuesOUT =
                (SELECT COUNT(DISTINCT([', @column_name, ']))
                FROM [', @table_catalog, '].[', @table_schema, '].
                [', @table_name, ']
                WHERE [', @column_name, '] IS NOT NULL) ')
                IF (@data_type != 'bit')
                BEGIN
                SET @sql_statement =
                CONCAT(@sql_statement,
                'SET @minimum_valueOUT =
                CAST((SELECT MIN([', @column_name, '])
                FROM [', @table_catalog, '].[', @table_schema, '].
                [', @table_name, ']
                WHERE [', @column_name, '] IS NOT NULL)
                AS nvarchar(225))
                SET @maximum_valueOUT =
                CAST((SELECT MAX([', @column_name, '])
                FROM [', @table_catalog, '].[', @table_schema, '].
                [', @table_name, ']
                WHERE [', @column_name, '] IS NOT NULL)
                AS nvarchar(225))')
                END
                ELSE
                BEGIN
                SET @sql_statement =
                CONCAT(@sql_statement,
                'SET @minimum_valueOUT = NULL
                SET @maximum_valueOUT = NULL');
                END
                SET @param_definition = N'@null_countOUT int OUTPUT,
                @distinct_valuesOUT int OUTPUT,
                @minimum_valueOUT nvarchar(255) OUTPUT,
                @maximum_valueOUT nvarchar(255) OUTPUT';
                print(@sql_statement)
                EXECUTE sp_executesql	@sql_statement,
                @param_definition,
                @null_countOUT = @null_count OUTPUT,
                @distinct_valuesOUT = @distinct_values OUTPUT,
                @minimum_valueOUT = @minimum_value OUTPUT,
                @maximum_valueOUT = @maximum_value OUTPUT;
                UPDATE @T1
                SET NullCount = @null_count,
                DistinctValues = @distinct_values,
                MinimumValue = @minimum_value,
                MaximumValue = @maximum_value
                WHERE ColumnName = @column_name;
                FETCH NEXT FROM column_cursor
                INTO @column_name, @data_type;
                END
                CLOSE column_cursor;
                DEALLOCATE column_cursor;
                SELECT * FROM @T1;")
  data <- execute_sql(
    server = server,
    database = database,
    sql = sql,
    output = TRUE
  )
  data[data$DataType == "nvarchar(-1)", "DataType"] <- "nvarchar(max)"
  data
}



table_select_list <- function(columns) {
  select_list <- ""
  if (is.null(columns)) {
    select_list <- "*"
  } else {
    for (column in columns) {
      select_list <- paste0(select_list, "[", column, "], ")
    }
    select_list <- substr(select_list, 1, nchar(select_list) - 2)
  }
  select_list
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
  } else if (max_string > 50 & max_string <= 255) {
    "nvarchar(255)"
  } else if (max_string > 255 & max_string <= 4000) {
    "nvarchar(4000)"
  } else {
    "nvarchar(max)"
  }
}


r_to_sql_datatype <- function(col_v) {
  r_data_type <- class(col_v)
  if (r_data_type %in% c("character", "factor")) {
    col_v <- as.character(col_v) # to ensure factor cols are character
    max_string <- max(nchar(col_v))
  }
  switch(r_data_type,
    "numeric" = "float",
    "logical" = "bit",
    "character" = r_to_sql_character_sizes(max_string),
    "factor" = r_to_sql_character_sizes(max_string),
    "POSIXct" = "datetime2(3)",
    "POSIXlt" = "datetime2(3)",
    "integer" = "int"
  )
}

get_nvarchar_size <- function(input_char_type) {
  sub("\\)", "", unlist(strsplit(input_char_type, "\\("))[2])
}


compatible_character_cols <- function(existing_col_type,
                                      to_load_col_type) {
  # Only if both column datatypes contain "nvarchar" then proceed
  if (!(grepl("nvarchar", existing_col_type) & grepl(
    "nvarchar",
    to_load_col_type
  ))) {
    return("incompatible")
  }
  else {
    # Extract the nvarchar column size, e.g. 255 from nvarchar(255)
    existing_col_size <- get_nvarchar_size(existing_col_type)
    to_load_col_size <- get_nvarchar_size(to_load_col_type)
    if (existing_col_size == "max") {
      "compatible" # If existing is max then to load will be fine
    } else if (to_load_col_size == "max") {
      "resize" # If existing not max but to load is then must resize
    } else if (as.numeric(to_load_col_size)
    <= as.numeric(existing_col_size)) {
      "compatible" # If neither is max, but existing greater
      # than or equal to load then will be fine
    }
    else {
      return("resize") # If neither is max, but existing smaller
      # than to load then must resize
    }
  }
}

# function to format multiline message, stop, warn strings due to 80 char limit
format_message <- function(message_string) {
  strwrap(message_string, prefix = " ", initial = "")
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
