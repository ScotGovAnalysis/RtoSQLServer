library(RtoSQLServer)


# Prepare a data frame to load ----------------------------------------------
test_iris <- iris # Here simply demo of loading a copy of iris dataframe


# Set database connection info --------------------------------------------


server <- "s0196a\\ADM"
database <- "admdemothemeadmdemotopic"
schema <- "admdemodataitem"


# Write a new dataframe to the database -----------------------------------------

#  NOTE: When loading, make sure the table_name specified above only contains chracters, numbers and _ (this will be automatically corrected if not)


write_dataframe_to_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_iris",
  dataframe = test_iris,
  append_to_existing = FALSE, # Set as FALSE to overwrite table if exists
  batch_size = 1e5, # Set as any integer or leave as default if less than about 100 K rows
  versioned_table = FALSE
) # Unless definitely need history table, set as FALSE (default)


# Append to existing table in the database --------------------------------

# In this example we are just appending the same dataframe to the existing table.
# Must ensure the existing table and the dataframe to be appended hold the same columns
# If specified append_to_existing=FALSE then the existing table is overwritten

write_dataframe_to_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_iris",
  dataframe = test_iris,
  append_to_existing = TRUE, # Setting this as TRUE ensures append to existing - default is FALSE
  batch_size = 1e5, # Set as any integer or leave as default if less than about 100 K rows
  versioned_table = FALSE
) # Unless definitely need history table, set as FALSE (default)



# Read the SQL Server table into an R dataframe ---------------------------

test_iris_db <- read_table_from_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_iris"
)


# Remove the table from the database  -------------------------------------

drop_table_from_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_iris",
  versioned_table = FALSE
)


# Run other SQL -----------------------------------------------------------
# Can run SQL to select specific columns and return as DF

sql <- paste0("select Species, Sepal_Length from ", schema, ".test_iris")

execute_sql(
  server = server,
  database = database,
  sql = sql,
  output = TRUE
)

# Can also run SQL that does not return a DF if set output FALSE (default)

sql <- paste0("delete from ", schema, ".test_iris where Species = 'virginica'")

execute_sql(
  server = server,
  database = database,
  sql = sql,
  output = FALSE
)
