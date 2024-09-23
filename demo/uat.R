library(RtoSQLServer)

# test dataframe
test_df <- data.frame(
  id = 1:5,
  first_name = c("Alice", "Bob", "Charlie", "David", "Eva")
)

# second dataframe for appending
append_df <- data.frame(
  id = 6:7,
  first_name = c("Frank", "Grace"),
  new_column = c("NewValue1", "NewValue2")
)


# validate if a condition is met, otherwise stop the script
check_condition <- function(condition, message) {
  if (!condition) {
    stop(message)
  }
}

# check if table exists
check_if_exists <- function(server, database, schema, table_name) {
  tables <- show_schema_tables(server = server, database = database, schema = schema)
  table_name %in% tables$table
}

# fn perform all tests
run_tests <- function(server,
                      database,
                      schema,
                      versioned_table = FALSE) {
  # Create a connection ----
  con <- create_sqlserver_connection(server = server, database = database)
  check_condition(!is.null(con), "Failed to create connection to the database")


  table_name <- if (versioned_table) "test_versioned_table" else "test_table"
  table_name_exists <- check_if_exists(server, database, schema, table_name)

  renamed_table <- paste0("renamed_", table_name)
  renamed_exists <- check_if_exists(server, database, schema, renamed_table)

  # Check and drop renamed table if exists -----
  if (!versioned_table && renamed_exists) {
    drop_table_from_db(server = server, database = database, schema = schema, table_name = renamed_table)
  }

  # if table exists drop and over-write
  if ((versioned_table && !table_name_exists) || (!versioned_table)) {
    write_dataframe_to_db(
      server = server,
      database = database,
      schema = schema,
      table_name = table_name,
      dataframe = test_df,
      versioned_table = versioned_table
    )
  } else { # if versioned and exists already delete rows and append instead
    delete_table_rows(server, database, schema, table_name)
    write_dataframe_to_db(
      server = server,
      database = database,
      schema = schema,
      table_name = table_name,
      dataframe = test_df,
      versioned_table = versioned_table,
      append_to_existing = TRUE
    )
  }
  # Check that the table was created successfully by reading it
  retrieved_df <- read_table_from_db(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name
  )
  check_condition(!is.null(retrieved_df), "Failed to retrieve table data")
  check_condition(nrow(retrieved_df) == 5, "Expected 5 rows in the table")

  # Run append_to_existing test: add new column and load rows
  write_dataframe_to_db(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name,
    dataframe = append_df,
    append_to_existing = TRUE
  )

  # Check if the new column "new_column" was added
  metadata <- db_table_metadata(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name
  )
  check_condition("new_column" %in% metadata$column_name, "Failed to add
                  'new_column'")

  # Check that the new rows were appended correctly
  retrieved_df <- read_table_from_db(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name
  )
  check_condition(nrow(retrieved_df) == 7, "Expected 7 rows after \\
                  appending new data")
  check_condition(all(retrieved_df$id == c(1:7)), "Appended rows do \\
                  not match expected values")


  # 2. Add a new column
  add_column(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name,
    column_name = "age",
    sample_value = 12
  )

  # Check if the column was added successfully
  metadata <- db_table_metadata(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name
  )
  check_condition("age" %in% metadata$column_name, "Failed to add 'age' column")

  # 3. Drop the newly added column
  drop_column(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name,
    column_name = "age"
  )

  # Check if the column was dropped successfully
  metadata <- db_table_metadata(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name
  )
  check_condition(!"age" %in% metadata$column_name, "Failed to \\
                  drop 'age' column")

  # 4. Rename a column
  rename_column(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name,
    old_column_name = "first_name",
    new_column_name = "first_name_only"
  )

  # Check if the column was renamed successfully
  metadata <- db_table_metadata(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name
  )
  check_condition("first_name_only" %in% metadata$column_name, "Failed to \\
                  rename 'first_name' to 'first_name_only'")

  # rename back again for not overwrite versioned scenario
  rename_column(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name,
    old_column_name = "first_name_only",
    new_column_name = "first_name"
  )

  # 5. Rename the table
  new_table_name <- paste0("renamed_", table_name)
  rename_table(
    server = server,
    database = database,
    schema = schema,
    old_table_name = table_name,
    new_table_name = renamed_table
  )

  # Check if the table was renamed successfully
  tables <- show_schema_tables(
    server = server,
    database = database,
    schema = schema
  )
  check_condition(new_table_name %in% tables$table, "Failed to \\
                    rename the table")

  rename_table(
    server = server,
    database = database,
    schema = schema,
    old_table_name = renamed_table,
    new_table_name = table_name
  )

  # 6. Delete rows where id = 5
  delete_table_rows(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name,
    filter_stmt = "id == 5"
  )

  # Verify that the row with id = 5 is deleted
  retrieved_df <- read_table_from_db(
    server = server,
    database = database,
    schema = schema,
    table_name = table_name
  )
  check_condition(!5 %in% retrieved_df$id, "Failed to delete the \\
                  row with id = 5")
  if (!versioned_table) {
    # 7. Drop the renamed table
    drop_table_from_db(
      server = server,
      database = database,
      schema = schema,
      table_name = table_name
    )

    # Check that the table was dropped
    tables <- show_schema_tables(
      server = server,
      database = database,
      schema = schema
    )
    check_condition(!new_table_name %in% tables$table_name, "Failed to drop \\
                    the table")
  }
  message(glue::glue("tests ran successfully for {database}.{schema}"))
  # Disconnect from the database
  DBI::dbDisconnect(con)
}

# Set database connection details for use in functions:
server <- "my_server//instance"
database <- "my_database"
schema <- "my_schema"

# Run the tests without versioning
run_tests(server, database, schema, versioned_table = FALSE)

# Run the tests with versioning enabled
run_tests(server, database, schema, versioned_table = TRUE)
