library(RtoSQLServerVersioning) # Custom package for SQL Server Data Loading. NOTE: Needs ODBC library v1.33, not v1.32 or earlier.
library(readr) # Better csv reading
library(stringr) # Easier string manipulation

# Example of loading a dataset that is refreshed daily by a separate process - the current date is used to find file to load to DB

# Set source folder -------------------------------------------------------

date_today <- Sys.Date()

source_files_folder <- paste0("//my_server/my_folder/mysubfolder/daily/", date_today, "/whole-data-sets")

# read and check latest source data --------------------------------------------------

file_prefix <- "daily-dataset-"

# ODBC does not like writing tables with "-" characters, so replace with "_" (also needed on dates)
db_prefix <- str_replace_all(file_prefix, "-", "_")

csv_file <- paste0(file_prefix, date_today, ".csv")

csv_fp <- file.path(source_files_folder, csv_file)

if (file.exists(csv_fp)) {
  source_df <- read_csv(file.path(source_files_folder, csv_file))

  # Uncomment below if wish to test with a subset first of all
  # source_df <- head(source_df, 10)

  # Check datatype of each column in df and subset to character cols
  cols_all <- sapply(source_df, class)
  cols_character <- cols_all[cols_all == "character"]

  # Truncate character columns to max of 255 characters
  for (col_name in names(cols_character)) {
    source_df[[col_name]] <- substr(source_df[[col_name]], start = 1, stop = 255)
  }


  # Database connection info ------------------------------------------------

  # Set connection details for use in functions:
  server <- "myserver\\myinstance"
  database <- "mydatabase"
  schema <- "myschema"

  # OPTIONAL remove previous day table if exists -----------------------------------------------

  db_yesterday <- str_replace_all(date_today - 1, "-", "_")


  try(drop_table_from_db(
    server = server,
    database = database,
    schema = schema,
    table_name = paste0(db_prefix, db_yesterday)
  ))


  # Write current date dataframe to db -------------------------------------------------------


  # Write the dataframe to a SQL Server table in batches of 100,000 rows at a time
  write_dataframe_to_db(
    database = database,
    server = server,
    schema = schema,
    table_name = paste0(db_prefix, str_replace_all(date_today, "-", "_")),
    dataframe = source_df,
    batch_size = 1e5,
    versioned_table = FALSE
  )
} else {
  stop(paste("csv file for current date:", csv_fp, "does not exist"))
}
