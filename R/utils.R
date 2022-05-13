create_sqlserver_connection <- function(database, server){
  tryCatch({
    odbc::dbConnect(
      odbc::odbc(),
      Driver = "SQL Server",
      Trusted_Connection = "True",
      DATABASE = database,
      SERVER = server)},
    error = function(cond) {
      stop(paste0("Failed to create connection to database: '", database, "' on server: '", server, "'\nOriginal error message: '", cond, "'"))
    })
}



execute_sql <- function(database, server, sql, output = FALSE, disconnect = TRUE) {
  output_data <- NULL
  if (! exists("connection")) {
       connection <- create_sqlserver_connection(database = database, server = server)}
  for(i in 1:length(sql)){
    if(output){
      output_data <- DBI::dbGetQuery(connection, sql[i])
    } else {
      DBI::dbGetQuery(connection, sql[i])
    }
  }
  if(disconnect) DBI::dbDisconnect(connection)
  return(output_data)
}



db_table_metadata <- function(database, server, schema, table_name) {
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
  data <- execute_sql(database = database, server = server, sql = sql, output = TRUE)
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



get_db_tables <- function(database, server) {
  sql <- "SELECT SCHEMA_NAME(t.schema_id) AS 'Schema',
  t.name AS 'Name'
  FROM sys.tables t
  WHERE t.temporal_type != 1"
  data <- execute_sql(database = database, server = server, sql = sql, output = TRUE)
  data
}

r_to_sql_data_type <- function(r_data_type) {
  sql_data_type <- switch(r_data_type,
                          "numeric" = "float",
                          "logical" = "bit",
                          "character" = "varchar(255)",
                          "factor" = "varchar(255)",
                          "POSIXct" = "datetime2(3)",
                          "POSIXlt" = "datetime2(3)",
                          "integer" = "int")
  sql_data_type
}