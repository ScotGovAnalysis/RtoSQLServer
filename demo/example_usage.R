library(RtoSQLServerVersioning)
library(readr)

# read check source data --------------------------------------------------

visa_df <- read_csv("C:/repos/visa-applications-2022-07-13.csv")

# Uncomment below if wish to test with a subset first of all
#visa_df <- head(visa_df, 10)

# Check datatype of each column in df and subset to character cols
cols_all <- sapply(visa_df, class)
cols_character <- cols_all[cols_all=="character"]

# Truncate character columns to max of 255 characters
for (col_name in names(cols_character)){
  visa_df[[col_name]] <- sapply(visa_df[[col_name]], substr, start=1, stop=255)
}


# Database connection info ------------------------------------------------


#Set connection details for use in functions:
server <- "s0855a\\DBXED"
database <- "h4u"
schema <- "daily"


# Write table to db -------------------------------------------------------


#Write the visa dataframe to a SQL Server table in batches of 100,000 rows at a time
write_dataframe_to_db(database=database, server=server, schema=schema, table_name="test_tom", dataframe=visa_df, batch_size=1e5, versioned_table=FALSE)


#Optional drop the table from the database
#drop_table_from_db(database=database, server=server, schema=schema, table_name="test_tom", versioned_table=FALSE)

