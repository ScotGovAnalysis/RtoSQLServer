create_sqlserver_connection <- function(server, database) {
  tryCatch(
    {
      odbc::dbConnect(
        odbc::odbc(),
        Driver = "SQL Server",
        Trusted_Connection = "True",
        DATABASE = database,
        SERVER = server
      )
    },
    error = function(cond) {
      stop(paste0("Failed to create connection to database: '", database, "' on server: '", server, "'\nOriginal error message: '", cond, "'"))
    }
  )
}



#' Connect, execute SQL in SQL Server database and then (optionally) disconnect from database.
#'
#' @param server Server instance where SQL Server database running.
#' @param database SQL Server database in which SQL executed.
#' @param sql SQL to be executed in the database.
#' @param output If TRUE write output of SQL to Dataframe. Defaults to FALSE.
#' @param disconnect If TRUE disconnect from SQL Server database at end. Defaults to TRUE.
#'
#' @return Dataframe or NULL depending on SQL executed.
#' @export
#'
#' @examples
#' sql_to_run <- "select test_column, other_column from my_test_table where other_column > 10"
#' execute_sql(database = my_database, server = my_server, sql = sql_to_run, output = TRUE, disconnect = TRUE)
execute_sql <- function(server, database, sql, output = FALSE, disconnect = TRUE) {
  output_data <- NULL
  connection <- create_sqlserver_connection(server = server, database = database)
  for (i in 1:length(sql)) {
    if (output) {
      tryCatch(
        {
          output_data <- DBI::dbGetQuery(connection, sql[i])
        },
        error = function(cond) {
          stop(paste0("Failed to execute SQL.\nOriginal error message: ", cond))
        }
      )
    } else {
      tryCatch(
        {
          DBI::dbGetQuery(connection, sql[i])
        },
        error = function(cond) {
          stop(paste0("Failed to execute SQL.\nOriginal error message: ", cond))
        }
      )
    }
  }
  if (disconnect) DBI::dbDisconnect(connection)
  return(output_data)
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
                REPLACE(CONCAT(DATA_TYPE, '(', CHARACTER_MAXIMUM_LENGTH, ')'), '()', '')
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
                FROM [', @table_catalog, '].[', @table_schema, '].[', @table_name, ']
                WHERE [', @column_name, '] IS NULL)
                SET @distinct_valuesOUT =
                (SELECT COUNT(DISTINCT([', @column_name, ']))
                FROM [', @table_catalog, '].[', @table_schema, '].[', @table_name, ']
                WHERE [', @column_name, '] IS NOT NULL) ')
                IF (@data_type != 'bit')
                BEGIN
                SET @sql_statement =
                CONCAT(@sql_statement,
                'SET @minimum_valueOUT =
                CAST((SELECT MIN([', @column_name, '])
                FROM [', @table_catalog, '].[', @table_schema, '].[', @table_name, ']
                WHERE [', @column_name, '] IS NOT NULL)
                AS nvarchar(225))
                SET @maximum_valueOUT =
                CAST((SELECT MAX([', @column_name, '])
                FROM [', @table_catalog, '].[', @table_schema, '].[', @table_name, ']
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
  data <- execute_sql(server = server, database = database, sql = sql, output = TRUE)
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



table_where_clause <- function(id_column, start_row, end_row) {
  dplyr::case_when(
    is.null(start_row) & is.null(end_row) ~ "",
    is.null(start_row) & !is.null(end_row) ~ paste0(" WHERE [", id_column, "] <= ", end_row),
    !is.null(start_row) & is.null(end_row) ~ paste0(" WHERE [", id_column, "] >= ", start_row),
    !is.null(start_row) & !is.null(end_row) ~ paste0(" WHERE [", id_column, "] BETWEEN ", start_row, " AND ", end_row)
  )
}



get_db_tables <- function(server, database) {
  sql <- "SELECT SCHEMA_NAME(t.schema_id) AS 'Schema',
  t.name AS 'Name'
  FROM sys.tables t"
  data <- execute_sql(database = database, server = server, sql = sql, output = TRUE)
  data
}

r_to_sql_character_sizes <- function(max_string) {
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


compatible_character_cols <- function(existing_col_type, to_load_col_type) {
  # Only both column datatypes contain "nvarchar" then proceed
  if (!(grepl("nvarchar", existing_col_type) & grepl("nvarchar", to_load_col_type))) {
    return("incompatible")
  }
  else {
    # Extract the nvarchar column size, e.g. 255 from nvarchar(255)
    existing_col_size <- as.numeric(gsub("[^0-9]", "", existing_col_type))
    to_load_col_size <- as.numeric(gsub("[^0-9]", "", to_load_col_type))

    if (to_load_col_size <= existing_col_size) {
      return("compatible") # No change needed
    }
    else {
      return("resize") # Indicates will need alter statement
    }
  }
}
