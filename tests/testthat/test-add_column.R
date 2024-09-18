test_that("create_add_column_sql generates correct SQL statement", {
  # Mock the quoted_schema_tbl function
  mockery::stub(
    create_add_column_sql, "quoted_schema_tbl",
    DBI::SQL("\"test_schema\".\"test_tbl\"")
  )

  sql <- create_add_column_sql(
    "test_schema",
    "test_tbl",
    "new_column",
    "VARCHAR(255)"
  )
  expected_sql <- glue::glue_sql("ALTER TABLE \"test_schema\".\"test_tbl\" \\
                                 ADD \"new_column\" VARCHAR(255) NULL;")
  expect_equal(sql, expected_sql)
})

test_that("validate_column_type handles valid and invalid inputs", {
  expect_equal(validate_column_type(123), "numeric")
  expect_equal(validate_column_type("test"), "character")

  # Test for invalid inputs (e.g., function name)
  expect_error(
    validate_column_type(character),
    "Invalid input: column_data cannot be a function name."
  )

  # Test for NULL input
  expect_null(validate_column_type(NULL))
})

test_that("clean_new_column_name handles invalid column names", {
  # Test for valid column name
  expect_equal(
    clean_new_column_name(
      "test_table",
      "valid_column"
    ),
    "valid_column"
  )

  # Reserved words in col name
  expect_equal(
    rename_reserved_column("SysEndTime",
      "test_tbl",
      suffix = "_new"
    ),
    "SysEndTime_new"
  )

  # Test for truncating long column name to 126 characters
  long_column_name <- paste0(rep("a", 130), collapse = "")
  truncated_column_name <- substr(long_column_name, 1, 126)

  expect_warning(
    truncated_name <- clean_new_column_name("test_table", long_column_name),
    glue::glue("Column name {long_column_name} is \\
               invalid using {truncated_column_name} instead.")
  )
  expect_equal(truncated_name, truncated_column_name)

  expect_warning(
    cleaned_name <- clean_new_column_name(
      "test_table",
      "column.name"
    ),
    glue::glue("Column name column.name is invalid \\
                            using column_name instead.")
  )

  # Test for replacing dots in column name
  expect_equal(cleaned_name, "column_name")
})

test_that("add_column stops if table does not exist", {
  # Mock check_table_exists to return FALSE
  mockery::stub(add_column, "check_table_exists", FALSE)

  expect_error(
    add_column("server", "database", "schema", "table_name", "new_column"),
    "Table: schema.table_name does not exist in the database."
  )
})

test_that("add_column stops if column already exists", {
  # Mock check_table_exists to return TRUE and db_table_metadata
  # to simulate an existing column
  mockery::stub(add_column, "check_table_exists", TRUE)
  mockery::stub(
    add_column, "db_table_metadata",
    data.frame(column_name = "existing_column")
  )

  expect_error(
    add_column("server", "database", "schema", "table_name", "existing_column"),
    "Column existing_column already exists in schema.table_name."
  )
})

test_that("add_column stops if no sql_data_type or sample_value provided", {
  # Mock check_table_exists and db_table_metadata for successful execution
  mockery::stub(add_column, "check_table_exists", TRUE)
  mockery::stub(
    add_column, "db_table_metadata",
    data.frame(column_name = "existing_column")
  )

  # Test for missing both sql_data_type and sample_value
  expect_error(
    add_column("server", "database", "schema", "table_name", "new_column"),
    glue::glue("You must provide either `sample_value` for \\
               data type inference or `sql_data_type` directly.")
  )
})

test_that("add_column generates and executes correct SQL", {
  # Mock dependencies for SQL generation and execution
  mockery::stub(add_column, "check_table_exists", TRUE)
  mockery::stub(
    add_column, "db_table_metadata",
    data.frame(column_name = "existing_column")
  )
  mockery::stub(add_column, "clean_new_column_name", "new_column")
  mockery::stub(add_column, "validate_column_type", TRUE)
  mockery::stub(add_column, "r_to_sql_data_type", "NVARCHAR(255)")
  mockery::stub(add_column, "create_add_column_sql", "SQL_QUERY")
  mockery::stub(add_column, "execute_sql", TRUE)

  # Test for valid execution
  expect_message(
    add_column("server",
      "database",
      "schema",
      "table_name",
      "new_column",
      sample_value = character()
    ),
    "Column new_column of type NVARCHAR(255) added to schema.table_name.",
    fixed = TRUE
  )
})
